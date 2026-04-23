//! Multi-reactor smoke test — N reactor threads share a port via
//! SO_REUSEPORT and the kernel load-balances incoming connections.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ssd_kv::engine::Index;
use ssd_kv::server::{start_reactor_multi, DatabaseManager, DbHandler, Handler, ServerTuning};
use ssd_kv::storage::{FileManager, WriteBuffer};

use tempfile::tempdir;

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

fn build_db_manager(path: &std::path::Path) -> Arc<DatabaseManager> {
    let fm = Arc::new(FileManager::new(path).unwrap());
    fm.create_file().unwrap();
    let idx = Arc::new(Index::new());
    let wb = Arc::new(WriteBuffer::new(fm.file_count() as u32, 1023));
    let handler = Arc::new(Handler::new(Arc::clone(&idx), Arc::clone(&fm), Arc::clone(&wb)));
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

fn wait_for_ready(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            return s;
        }
        if Instant::now() > deadline {
            panic!("multi-reactor not ready on :{}", port);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn read_at_least(stream: &mut TcpStream, n: usize, deadline: Instant) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    while buf.len() < n && Instant::now() < deadline {
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(m) => buf.extend_from_slice(&tmp[..m]),
            Err(_) => continue,
        }
    }
    buf
}

#[test]
fn four_reactors_share_a_port_and_all_serve_requests() {
    if !cfg!(target_os = "linux") {
        return;
    }

    let dir = tempdir().unwrap();
    let db_manager = build_db_manager(dir.path());

    let port = free_port();
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Ask for 4 reactors; they must share the port via SO_REUSEPORT.
    let _h = start_reactor_multi(
        addr,
        db_manager,
        None,
        false,
        ServerTuning::default_test(),
        4,
    );

    // Give the reactors a moment to bind.
    std::thread::sleep(Duration::from_millis(100));

    // Fan out 16 clients; with 4 reactors the kernel will spread them.
    // Every client performs the same SET/GET round trip against the
    // shared keyspace. Either all succeed (every reactor is serving) or
    // some fail (one of the reactors died or couldn't bind).
    let mut handles = Vec::new();
    for i in 0..16u32 {
        handles.push(std::thread::spawn(move || {
            let mut stream = wait_for_ready(port);
            stream.set_nodelay(true).unwrap();

            let key = format!("client-{}", i);
            let val = format!("value-{}", i);
            stream
                .write_all(&resp_array(&[b"SET", key.as_bytes(), val.as_bytes()]))
                .unwrap();
            stream
                .write_all(&resp_array(&[b"GET", key.as_bytes()]))
                .unwrap();

            // Expect +OK\r\n then $6\r\nvalue-*\r\n (total ~ 5 + 12 bytes)
            let deadline = Instant::now() + Duration::from_secs(3);
            let reply = read_at_least(&mut stream, 16, deadline);

            assert!(
                reply.starts_with(b"+OK\r\n"),
                "client {} got unexpected SET reply: {:?}",
                i,
                String::from_utf8_lossy(&reply)
            );
            assert!(
                reply.windows(val.len()).any(|w| w == val.as_bytes()),
                "client {} missing its value in reply: {:?}",
                i,
                String::from_utf8_lossy(&reply)
            );
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn single_reactor_mode_still_works_after_multi_refactor() {
    if !cfg!(target_os = "linux") {
        return;
    }

    let dir = tempdir().unwrap();
    let db_manager = build_db_manager(dir.path());
    let port = free_port();
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();

    // n=1 goes through the same multi code path but with reuse_port=false.
    let _h = start_reactor_multi(
        addr,
        db_manager,
        None,
        false,
        ServerTuning::default_test(),
        1,
    );

    let mut stream = wait_for_ready(port);
    stream.set_nodelay(true).unwrap();

    stream.write_all(&resp_array(&[b"PING"])).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let reply = read_at_least(&mut stream, 7, deadline);
    assert!(reply.starts_with(b"+PONG\r\n"));
}
