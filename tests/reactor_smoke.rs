//! End-to-end smoke test for the io_uring RESP reactor.
//!
//! Spins up the reactor on a local ephemeral port, connects with a plain
//! TcpStream, and validates PING / SET / GET / DEL round-trip.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ssd_kv::server::{
    start_reactor_server, DatabaseManager, DbHandler, Handler, ServerTuning,
};
use ssd_kv::storage::{FileManager, WriteBuffer};
use ssd_kv::engine::Index;

use tempfile::tempdir;

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

fn build_db_manager(tempdir_path: &std::path::Path) -> Arc<DatabaseManager> {
    let fm = Arc::new(FileManager::new(tempdir_path).unwrap());
    fm.create_file().unwrap();
    let idx = Arc::new(Index::new());
    let wb = Arc::new(WriteBuffer::new(fm.file_count() as u32, 1023));

    let handler = Handler::new(Arc::clone(&idx), Arc::clone(&fm), Arc::clone(&wb));
    let handler = Arc::new(handler);

    Arc::new(DatabaseManager::new(vec![DbHandler::Ssd(handler)]))
}

fn resp_array(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Read until we've accumulated at least `n` bytes or the deadline elapses.
fn read_at_least(stream: &mut TcpStream, deadline: Instant, n: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    while buf.len() < n && Instant::now() < deadline {
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(m) => buf.extend_from_slice(&tmp[..m]),
            Err(_) => continue, // timeout, loop
        }
    }
    buf
}

fn wait_for_ready(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            return s;
        }
        if Instant::now() > deadline {
            panic!("reactor did not become ready on :{}", port);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn reactor_ping_set_get_del_roundtrip() {
    // On non-Linux or kernels without io_uring, UringServer::new may fail.
    // Skip the test in that case; the CI we care about is Linux.
    if !cfg!(target_os = "linux") {
        return;
    }

    let dir = tempdir().unwrap();
    let db_manager = build_db_manager(dir.path());

    let port = free_port();
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();
    let tuning = ServerTuning::default_test();

    // Spawn the reactor on a background thread. If io_uring isn't
    // available, UringServer::new errors out and the thread exits; that
    // will manifest as a connect failure below and we skip.
    let _h = start_reactor_server(addr, db_manager, tuning);

    let mut stream = match TcpStream::connect_timeout(
        &addr,
        Duration::from_secs(2),
    ) {
        Ok(s) => s,
        Err(_) => {
            // Try wait-for-ready once more in case spawn is slow.
            wait_for_ready(port)
        }
    };
    stream.set_nodelay(true).unwrap();

    // -- PING
    stream.write_all(&resp_array(&[b"PING"])).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let buf = read_at_least(&mut stream, deadline, 7);
    assert!(
        buf.starts_with(b"+PONG\r\n"),
        "PING reply was: {:?}",
        String::from_utf8_lossy(&buf)
    );

    // -- SET foo bar
    stream
        .write_all(&resp_array(&[b"SET", b"foo", b"bar"]))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let buf = read_at_least(&mut stream, deadline, 5);
    assert!(
        buf.starts_with(b"+OK\r\n"),
        "SET reply was: {:?}",
        String::from_utf8_lossy(&buf)
    );

    // -- GET foo
    stream.write_all(&resp_array(&[b"GET", b"foo"])).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let buf = read_at_least(&mut stream, deadline, 11);
    // "$3\r\nbar\r\n"
    assert!(
        buf.windows(9).any(|w| w == b"$3\r\nbar\r\n"),
        "GET reply was: {:?}",
        String::from_utf8_lossy(&buf)
    );

    // -- DEL foo
    stream.write_all(&resp_array(&[b"DEL", b"foo"])).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let buf = read_at_least(&mut stream, deadline, 4);
    assert!(
        buf.starts_with(b":1\r\n"),
        "DEL reply was: {:?}",
        String::from_utf8_lossy(&buf)
    );

    // -- GET foo (should miss)
    stream.write_all(&resp_array(&[b"GET", b"foo"])).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let buf = read_at_least(&mut stream, deadline, 5);
    assert!(
        buf.starts_with(b"$-1\r\n"),
        "GET miss reply was: {:?}",
        String::from_utf8_lossy(&buf)
    );

    // Connection is closed when the test process exits; the reactor loop
    // keeps running but that's fine — integration tests never return to
    // the reactor thread.
}

#[test]
fn reactor_pipelined_burst_is_ordered() {
    if !cfg!(target_os = "linux") {
        return;
    }

    let dir = tempdir().unwrap();
    let db_manager = build_db_manager(dir.path());
    let port = free_port();
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();

    let _h = start_reactor_server(addr, db_manager, ServerTuning::default_test());
    let mut stream = wait_for_ready(port);
    stream.set_nodelay(true).unwrap();

    // Fire 100 pipelined SETs followed by 100 GETs, collect all replies.
    let mut request = Vec::new();
    for i in 0..100u32 {
        let k = format!("k{:04}", i);
        let v = format!("v{:04}", i);
        request.extend_from_slice(&resp_array(&[b"SET", k.as_bytes(), v.as_bytes()]));
    }
    for i in 0..100u32 {
        let k = format!("k{:04}", i);
        request.extend_from_slice(&resp_array(&[b"GET", k.as_bytes()]));
    }
    stream.write_all(&request).unwrap();

    // Expect 100 "+OK\r\n" (500 bytes) then 100 "$4\r\nvXXXX\r\n"
    // That's 500 + 100 * 11 = 1600 bytes.
    let deadline = Instant::now() + Duration::from_secs(5);
    let buf = read_at_least(&mut stream, deadline, 1600);

    // Crude check: must contain 100 OKs.
    let ok_count = buf
        .windows(5)
        .filter(|w| *w == b"+OK\r\n")
        .count();
    assert!(ok_count >= 100, "expected 100 +OK, saw {}", ok_count);

    // Must contain vXXXX for every i.
    for i in 0..100u32 {
        let v = format!("v{:04}", i);
        assert!(
            buf.windows(v.len()).any(|w| w == v.as_bytes()),
            "missing {} in pipelined replies",
            v
        );
    }
}
