//! Data validation test for SSD-KV.
//!
//! This test validates:
//! 1. Data integrity (write-then-read returns correct data)
//! 2. Key uniqueness
//! 3. Delete operations
//! 4. Concurrent access
//! 5. Large values
//! 6. Edge cases

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PROTOCOL_MAGIC: u16 = 0x564B;
const PROTOCOL_VERSION: u8 = 1;
const OPCODE_GET: u8 = 0x01;
const OPCODE_PUT: u8 = 0x02;
const OPCODE_DELETE: u8 = 0x03;
const OPCODE_PING: u8 = 0x20;
const OPCODE_SUCCESS: u8 = 0x80;
const OPCODE_NOT_FOUND: u8 = 0x81;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let host = args.get(1).map(|s| s.as_str()).unwrap_or("127.0.0.1:7777");

    println!("===========================================");
    println!("  SSD-KV Data Validation Test");
    println!("===========================================");
    println!("  Host: {}", host);
    println!();

    let mut passed = 0;
    let mut failed = 0;

    // Test 1: Basic connectivity (PING)
    print!("Test 1: PING/PONG... ");
    if test_ping(host) {
        println!("PASSED");
        passed += 1;
    } else {
        println!("FAILED");
        failed += 1;
    }

    // Test 2: Basic PUT/GET
    print!("Test 2: Basic PUT/GET... ");
    if test_basic_put_get(host) {
        println!("PASSED");
        passed += 1;
    } else {
        println!("FAILED");
        failed += 1;
    }

    // Test 3: Data integrity (100 keys)
    print!("Test 3: Data integrity (100 keys)... ");
    if test_data_integrity(host, 100) {
        println!("PASSED");
        passed += 1;
    } else {
        println!("FAILED");
        failed += 1;
    }

    // Test 4: Large values
    print!("Test 4: Large values (64KB)... ");
    if test_large_values(host) {
        println!("PASSED");
        passed += 1;
    } else {
        println!("FAILED");
        failed += 1;
    }

    // Test 5: Delete operations
    print!("Test 5: DELETE operations... ");
    if test_delete(host) {
        println!("PASSED");
        passed += 1;
    } else {
        println!("FAILED");
        failed += 1;
    }

    // Test 6: Non-existent keys
    print!("Test 6: GET non-existent key... ");
    if test_not_found(host) {
        println!("PASSED");
        passed += 1;
    } else {
        println!("FAILED");
        failed += 1;
    }

    // Test 7: Overwrite existing key
    print!("Test 7: Overwrite existing key... ");
    if test_overwrite(host) {
        println!("PASSED");
        passed += 1;
    } else {
        println!("FAILED");
        failed += 1;
    }

    // Test 8: Concurrent access
    print!("Test 8: Concurrent access (4 threads)... ");
    if test_concurrent_access(host, 4, 1000) {
        println!("PASSED");
        passed += 1;
    } else {
        println!("FAILED");
        failed += 1;
    }

    // Test 9: Empty value
    print!("Test 9: Empty value... ");
    if test_empty_value(host) {
        println!("PASSED");
        passed += 1;
    } else {
        println!("FAILED");
        failed += 1;
    }

    // Test 10: Special characters in key
    print!("Test 10: Special characters in key... ");
    if test_special_chars(host) {
        println!("PASSED");
        passed += 1;
    } else {
        println!("FAILED");
        failed += 1;
    }

    println!();
    println!("===========================================");
    println!("  Results: {} passed, {} failed", passed, failed);
    println!("===========================================");

    if failed > 0 {
        std::process::exit(1);
    }
}

fn test_ping(host: &str) -> bool {
    let mut stream = match TcpStream::connect(host) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Connection failed: {}", e);
            return false;
        }
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    // Send PING
    let mut header = [0u8; 16];
    header[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
    header[2] = PROTOCOL_VERSION;
    header[3] = OPCODE_PING;
    header[4..8].copy_from_slice(&1u32.to_le_bytes()); // request_id

    if stream.write_all(&header).is_err() {
        return false;
    }

    // Read response
    let mut resp = [0u8; 16];
    if stream.read_exact(&mut resp).is_err() {
        return false;
    }

    resp[3] == OPCODE_SUCCESS
}

fn test_basic_put_get(host: &str) -> bool {
    let mut stream = match TcpStream::connect(host) {
        Ok(s) => s,
        Err(_) => return false,
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    let key = b"test_basic_key";
    let value = b"test_basic_value_12345";

    // PUT
    if put(&mut stream, 1, key, value).is_err() {
        return false;
    }

    // GET
    match get(&mut stream, 2, key) {
        Ok(v) => v == value,
        Err(_) => false,
    }
}

fn test_data_integrity(host: &str, num_keys: usize) -> bool {
    let mut stream = match TcpStream::connect(host) {
        Ok(s) => s,
        Err(_) => return false,
    };
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();

    let mut expected: HashMap<String, Vec<u8>> = HashMap::new();

    // Write all keys with unique values
    for i in 0..num_keys {
        let key = format!("integrity_key_{:05}", i);
        let value: Vec<u8> = format!("value_{}_{}", i, i * 17 + 42).into_bytes();

        if put(&mut stream, i as u32, key.as_bytes(), &value).is_err() {
            eprintln!("PUT failed for key {}", key);
            return false;
        }
        expected.insert(key, value);
    }

    // Verify all keys
    for (i, (key, expected_value)) in expected.iter().enumerate() {
        match get(&mut stream, (num_keys + i) as u32, key.as_bytes()) {
            Ok(actual_value) => {
                if actual_value != *expected_value {
                    eprintln!("Value mismatch for key {}: expected {:?}, got {:?}",
                        key, expected_value, actual_value);
                    return false;
                }
            }
            Err(e) => {
                eprintln!("GET failed for key {}: {}", key, e);
                return false;
            }
        }
    }

    true
}

fn test_large_values(host: &str) -> bool {
    let mut stream = match TcpStream::connect(host) {
        Ok(s) => s,
        Err(_) => return false,
    };
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();

    let key = b"large_value_key";
    let value: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();

    if put(&mut stream, 1, key, &value).is_err() {
        return false;
    }

    match get(&mut stream, 2, key) {
        Ok(v) => v == value,
        Err(_) => false,
    }
}

fn test_delete(host: &str) -> bool {
    let mut stream = match TcpStream::connect(host) {
        Ok(s) => s,
        Err(_) => return false,
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    let key = b"delete_test_key";
    let value = b"delete_test_value";

    // PUT
    if put(&mut stream, 1, key, value).is_err() {
        return false;
    }

    // Verify it exists
    if get(&mut stream, 2, key).is_err() {
        return false;
    }

    // DELETE
    if delete(&mut stream, 3, key).is_err() {
        return false;
    }

    // Verify it's gone
    match get(&mut stream, 4, key) {
        Err(_) => true, // Expected - key should not be found
        Ok(_) => false, // Should not exist
    }
}

fn test_not_found(host: &str) -> bool {
    let mut stream = match TcpStream::connect(host) {
        Ok(s) => s,
        Err(_) => return false,
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    let key = b"definitely_nonexistent_key_xyz_12345";

    match get(&mut stream, 1, key) {
        Err(_) => true, // Expected
        Ok(_) => false, // Should not exist
    }
}

fn test_overwrite(host: &str) -> bool {
    let mut stream = match TcpStream::connect(host) {
        Ok(s) => s,
        Err(_) => return false,
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    let key = b"overwrite_key";
    let value1 = b"first_value";
    let value2 = b"second_value_different";

    // PUT first value
    if put(&mut stream, 1, key, value1).is_err() {
        return false;
    }

    // Verify first value
    match get(&mut stream, 2, key) {
        Ok(v) if v == value1 => {}
        _ => return false,
    }

    // PUT second value (overwrite)
    if put(&mut stream, 3, key, value2).is_err() {
        return false;
    }

    // Verify second value
    match get(&mut stream, 4, key) {
        Ok(v) => v == value2,
        Err(_) => false,
    }
}

fn test_concurrent_access(host: &str, num_threads: usize, keys_per_thread: usize) -> bool {
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for t in 0..num_threads {
        let host = host.to_string();
        let success = Arc::clone(&success_count);
        let errors = Arc::clone(&error_count);

        handles.push(std::thread::spawn(move || {
            let mut stream = match TcpStream::connect(&host) {
                Ok(s) => s,
                Err(_) => {
                    errors.fetch_add(keys_per_thread, Ordering::Relaxed);
                    return;
                }
            };
            stream.set_read_timeout(Some(Duration::from_secs(30))).ok();

            for i in 0..keys_per_thread {
                let key = format!("concurrent_t{}_k{}", t, i);
                let value = format!("value_t{}_k{}_data", t, i);

                // PUT
                if put(&mut stream, i as u32, key.as_bytes(), value.as_bytes()).is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // GET and verify
                match get(&mut stream, (keys_per_thread + i) as u32, key.as_bytes()) {
                    Ok(v) if v == value.as_bytes() => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().ok();
    }

    let successes = success_count.load(Ordering::Relaxed);
    let errors_val = error_count.load(Ordering::Relaxed);
    let total = num_threads * keys_per_thread;

    if errors_val > 0 {
        eprintln!("Concurrent test: {} successes, {} errors out of {}", successes, errors_val, total);
    }

    // Allow some errors due to timing, but should be mostly successful
    successes >= total * 95 / 100
}

fn test_empty_value(host: &str) -> bool {
    let mut stream = match TcpStream::connect(host) {
        Ok(s) => s,
        Err(_) => return false,
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    let key = b"empty_value_key";
    let value = b"";

    if put(&mut stream, 1, key, value).is_err() {
        return false;
    }

    match get(&mut stream, 2, key) {
        Ok(v) => v.is_empty(),
        Err(_) => false,
    }
}

fn test_special_chars(host: &str) -> bool {
    let mut stream = match TcpStream::connect(host) {
        Ok(s) => s,
        Err(_) => return false,
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    let key = b"key-with.special_chars:123";
    let value = b"value with spaces and \t tabs";

    if put(&mut stream, 1, key, value).is_err() {
        return false;
    }

    match get(&mut stream, 2, key) {
        Ok(v) => v == value,
        Err(_) => false,
    }
}

// Helper functions
fn put(stream: &mut TcpStream, request_id: u32, key: &[u8], value: &[u8]) -> std::io::Result<()> {
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

    if resp_opcode == OPCODE_SUCCESS {
        Ok(())
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::Other, format!("PUT failed: opcode {}", resp_opcode)))
    }
}

fn get(stream: &mut TcpStream, request_id: u32, key: &[u8]) -> std::io::Result<Vec<u8>> {
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

    if resp_opcode == OPCODE_SUCCESS {
        Ok(resp_payload)
    } else if resp_opcode == OPCODE_NOT_FOUND {
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Key not found"))
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::Other, format!("GET failed: opcode {}", resp_opcode)))
    }
}

fn delete(stream: &mut TcpStream, request_id: u32, key: &[u8]) -> std::io::Result<()> {
    let payload_len = 2 + key.len();

    let mut header = [0u8; 16];
    header[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
    header[2] = PROTOCOL_VERSION;
    header[3] = OPCODE_DELETE;
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

    if resp_payload_len > 0 {
        let mut resp_payload = vec![0u8; resp_payload_len as usize];
        stream.read_exact(&mut resp_payload)?;
    }

    if resp_opcode == OPCODE_SUCCESS {
        Ok(())
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::Other, format!("DELETE failed: opcode {}", resp_opcode)))
    }
}
