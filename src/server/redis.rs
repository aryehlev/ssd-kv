//! High-performance Redis protocol (RESP) server.
//!
//! Optimizations:
//! - Pipelined command processing (batch multiple commands before flushing)
//! - Pre-allocated buffers for zero-copy
//! - Inline response building
//! - TCP_NODELAY for low latency

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, error, info, trace};

use crate::cluster::router::ClusterRouter;
use crate::server::handler::Handler;

/// Maximum pipeline depth before forcing flush
const MAX_PIPELINE_DEPTH: usize = 128;

/// Initial read buffer size (64KB - efficient for small values)
const INITIAL_READ_BUFFER_SIZE: usize = 64 * 1024;

/// Maximum read buffer size (64MB - supports arbitrarily large values)
const MAX_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// Write buffer size
const WRITE_BUFFER_SIZE: usize = 64 * 1024;

/// RESP data types
#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    /// Serializes the value to RESP format into a buffer
    #[inline]
    pub fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            RespValue::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                buf.push(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(i) => {
                buf.push(b':');
                buf.extend_from_slice(itoa::Buffer::new().format(*i).as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(data)) => {
                buf.push(b'$');
                buf.extend_from_slice(itoa::Buffer::new().format(data.len()).as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            RespValue::Array(Some(items)) => {
                buf.push(b'*');
                buf.extend_from_slice(itoa::Buffer::new().format(items.len()).as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.serialize_into(buf);
                }
            }
        }
    }

    /// Legacy serialize (allocating)
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        self.serialize_into(&mut buf);
        buf
    }

    #[inline]
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    #[inline]
    pub fn pong() -> Self {
        RespValue::SimpleString("PONG".to_string())
    }

    #[inline]
    pub fn null() -> Self {
        RespValue::BulkString(None)
    }

    #[inline]
    pub fn err(msg: &str) -> Self {
        RespValue::Error(format!("ERR {}", msg))
    }

    #[inline]
    pub fn bulk(data: Vec<u8>) -> Self {
        RespValue::BulkString(Some(data))
    }
}

/// Fast RESP parser with minimal allocations
pub struct RespParser {
    buf: Vec<u8>,
    pos: usize,
    len: usize,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            buf: vec![0u8; INITIAL_READ_BUFFER_SIZE],
            pos: 0,
            len: 0,
        }
    }

    /// Read more data from the stream, growing the buffer if needed
    #[inline]
    fn fill_buffer(&mut self, stream: &mut impl Read) -> io::Result<bool> {
        // Compact buffer if needed
        if self.pos > 0 {
            if self.pos < self.len {
                self.buf.copy_within(self.pos..self.len, 0);
                self.len -= self.pos;
            } else {
                self.len = 0;
            }
            self.pos = 0;
        }

        // Grow buffer if full after compaction
        if self.len == self.buf.len() {
            let new_size = (self.buf.len() * 2).min(MAX_BUFFER_SIZE);
            if new_size <= self.buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "RESP message exceeds maximum buffer size",
                ));
            }
            self.buf.resize(new_size, 0);
        }

        // Read more data
        let n = stream.read(&mut self.buf[self.len..])?;
        if n == 0 {
            return Ok(false); // EOF
        }
        self.len += n;
        Ok(true)
    }

    /// Shrink buffer back toward initial size when it's oversized and mostly empty.
    #[inline]
    fn maybe_shrink(&mut self) {
        let remaining = self.len - self.pos;
        // Only shrink if buffer is larger than initial and less than 1/4 used
        if self.buf.len() > INITIAL_READ_BUFFER_SIZE && remaining < self.buf.len() / 4 {
            let new_size = (self.buf.len() / 2).max(INITIAL_READ_BUFFER_SIZE);
            if remaining > 0 {
                self.buf.copy_within(self.pos..self.len, 0);
            }
            self.len = remaining;
            self.pos = 0;
            self.buf.truncate(new_size);
            self.buf.resize(new_size, 0);
        }
    }

    /// Try to parse a complete RESP value
    pub fn try_parse(&mut self, stream: &mut impl Read) -> io::Result<Option<RespValue>> {
        loop {
            if let Some(value) = self.parse_value()? {
                self.maybe_shrink();
                return Ok(Some(value));
            }

            // Need more data
            if !self.fill_buffer(stream)? {
                return Ok(None); // EOF
            }
        }
    }

    /// Parse a RESP value from the buffer
    fn parse_value(&mut self) -> io::Result<Option<RespValue>> {
        if self.pos >= self.len {
            return Ok(None);
        }

        let type_char = self.buf[self.pos];
        self.pos += 1;

        match type_char {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RESP type")),
        }
    }

    fn parse_line(&mut self) -> io::Result<Option<&[u8]>> {
        // Find \r\n
        let start = self.pos;
        while self.pos < self.len {
            if self.pos > 0 && self.buf[self.pos - 1] == b'\r' && self.buf[self.pos] == b'\n' {
                let line = &self.buf[start..self.pos - 1];
                self.pos += 1;
                return Ok(Some(line));
            }
            self.pos += 1;
        }
        self.pos = start;
        Ok(None)
    }

    fn parse_simple_string(&mut self) -> io::Result<Option<RespValue>> {
        match self.parse_line()? {
            Some(line) => Ok(Some(RespValue::SimpleString(
                String::from_utf8_lossy(line).into_owned()
            ))),
            None => {
                self.pos -= 1; // Unpush type char
                Ok(None)
            }
        }
    }

    fn parse_error(&mut self) -> io::Result<Option<RespValue>> {
        match self.parse_line()? {
            Some(line) => Ok(Some(RespValue::Error(
                String::from_utf8_lossy(line).into_owned()
            ))),
            None => {
                self.pos -= 1;
                Ok(None)
            }
        }
    }

    fn parse_integer(&mut self) -> io::Result<Option<RespValue>> {
        let start = self.pos;
        match self.parse_line()? {
            Some(line) => {
                let s = std::str::from_utf8(line)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
                let i = s.parse::<i64>()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid integer"))?;
                Ok(Some(RespValue::Integer(i)))
            }
            None => {
                self.pos = start - 1;
                Ok(None)
            }
        }
    }

    fn parse_bulk_string(&mut self) -> io::Result<Option<RespValue>> {
        let start = self.pos - 1;

        // Parse length line
        let len = match self.parse_line()? {
            Some(line) => {
                let s = std::str::from_utf8(line)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
                s.parse::<i64>()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk length"))?
            }
            None => {
                self.pos = start;
                return Ok(None);
            }
        };

        if len < 0 {
            return Ok(Some(RespValue::BulkString(None)));
        }

        let len = len as usize;

        // Check if we have enough data
        if self.pos + len + 2 > self.len {
            self.pos = start;
            return Ok(None);
        }

        let data = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len + 2; // Skip data and \r\n

        Ok(Some(RespValue::BulkString(Some(data))))
    }

    fn parse_array(&mut self) -> io::Result<Option<RespValue>> {
        let start = self.pos - 1;

        // Parse count line
        let count = match self.parse_line()? {
            Some(line) => {
                let s = std::str::from_utf8(line)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
                s.parse::<i64>()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid array length"))?
            }
            None => {
                self.pos = start;
                return Ok(None);
            }
        };

        if count < 0 {
            return Ok(Some(RespValue::Array(None)));
        }

        let mut items = Vec::with_capacity(count as usize);
        for _ in 0..count {
            match self.parse_value()? {
                Some(item) => items.push(item),
                None => {
                    self.pos = start;
                    return Ok(None);
                }
            }
        }

        Ok(Some(RespValue::Array(Some(items))))
    }
}

/// High-performance Redis command handler
pub struct RedisHandler {
    handler: Arc<Handler>,
    router: Option<Arc<ClusterRouter>>,
}

impl RedisHandler {
    pub fn new(handler: Arc<Handler>) -> Self {
        Self { handler, router: None }
    }

    pub fn with_router(handler: Arc<Handler>, router: Arc<ClusterRouter>) -> Self {
        Self {
            handler,
            router: Some(router),
        }
    }

    /// Handles a Redis command and writes response to buffer
    #[inline]
    pub fn handle_command(&self, value: RespValue, out: &mut Vec<u8>) {
        let args = match value {
            RespValue::Array(Some(items)) => items,
            _ => {
                RespValue::err("Expected array").serialize_into(out);
                return;
            }
        };

        if args.is_empty() {
            RespValue::err("Empty command").serialize_into(out);
            return;
        }

        // Extract command name
        let cmd = match &args[0] {
            RespValue::BulkString(Some(data)) => data,
            _ => {
                RespValue::err("Invalid command format").serialize_into(out);
                return;
            }
        };

        // Fast path for common commands
        match cmd.as_slice() {
            b"GET" | b"get" => self.cmd_get(&args, out),
            b"SET" | b"set" => self.cmd_set(&args, out),
            b"PING" | b"ping" => self.cmd_ping(&args, out),
            b"DEL" | b"del" => self.cmd_del(&args, out),
            b"MGET" | b"mget" => self.cmd_mget(&args, out),
            b"MSET" | b"mset" => self.cmd_mset(&args, out),
            b"EXISTS" | b"exists" => self.cmd_exists(&args, out),
            b"COMMAND" | b"command" => self.cmd_command(out),
            b"INFO" | b"info" => self.cmd_info(out),
            b"DBSIZE" | b"dbsize" => self.cmd_dbsize(out),
            b"CLUSTER" | b"cluster" => self.cmd_cluster(&args, out),
            _ => {
                let cmd_str = String::from_utf8_lossy(cmd);
                RespValue::err(&format!("Unknown command: {}", cmd_str)).serialize_into(out);
            }
        }
    }

    #[inline]
    fn cmd_ping(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() > 1 {
            if let RespValue::BulkString(Some(data)) = &args[1] {
                RespValue::BulkString(Some(data.clone())).serialize_into(out);
                return;
            }
        }
        RespValue::pong().serialize_into(out);
    }

    #[inline]
    fn cmd_get(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("GET requires 1 argument").serialize_into(out);
            return;
        }

        let key = match &args[1] {
            RespValue::BulkString(Some(data)) => data,
            _ => {
                RespValue::err("Invalid key").serialize_into(out);
                return;
            }
        };

        let result = if let Some(router) = &self.router {
            router.get(key)
        } else {
            self.handler.get_value(key)
        };

        match result {
            Some(value) => RespValue::bulk(value).serialize_into(out),
            None => RespValue::null().serialize_into(out),
        }
    }

    #[inline]
    fn cmd_set(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("SET requires 2 arguments").serialize_into(out);
            return;
        }

        let key = match &args[1] {
            RespValue::BulkString(Some(data)) => data,
            _ => {
                RespValue::err("Invalid key").serialize_into(out);
                return;
            }
        };

        let value = match &args[2] {
            RespValue::BulkString(Some(data)) => data,
            _ => {
                RespValue::err("Invalid value").serialize_into(out);
                return;
            }
        };

        // Parse optional EX/PX for TTL
        let ttl = self.parse_set_options(&args[3..]);

        let result = if let Some(router) = &self.router {
            router.put(key, value, ttl)
        } else {
            self.handler.put_sync(key, value, ttl)
        };

        match result {
            Ok(_) => RespValue::ok().serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    fn parse_set_options(&self, args: &[RespValue]) -> u32 {
        let mut i = 0;
        while i < args.len() {
            if let RespValue::BulkString(Some(opt)) = &args[i] {
                match opt.as_slice() {
                    b"EX" | b"ex" => {
                        if i + 1 < args.len() {
                            if let RespValue::BulkString(Some(val)) = &args[i + 1] {
                                if let Some(secs) = std::str::from_utf8(val)
                                    .ok()
                                    .and_then(|s| s.parse::<u32>().ok())
                                {
                                    return secs;
                                }
                            }
                        }
                    }
                    b"PX" | b"px" => {
                        if i + 1 < args.len() {
                            if let RespValue::BulkString(Some(val)) = &args[i + 1] {
                                if let Some(ms) = std::str::from_utf8(val)
                                    .ok()
                                    .and_then(|s| s.parse::<u32>().ok())
                                {
                                    return ms / 1000;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        0
    }

    #[inline]
    fn cmd_del(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("DEL requires at least 1 argument").serialize_into(out);
            return;
        }

        let mut deleted = 0i64;
        for arg in &args[1..] {
            if let RespValue::BulkString(Some(key)) = arg {
                let result = if let Some(router) = &self.router {
                    router.delete(key)
                } else {
                    self.handler.delete_sync(key)
                };
                if let Ok(true) = result {
                    deleted += 1;
                }
            }
        }

        RespValue::Integer(deleted).serialize_into(out);
    }

    #[inline]
    fn cmd_mget(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("MGET requires at least 1 argument").serialize_into(out);
            return;
        }

        let count = args.len() - 1;
        out.push(b'*');
        out.extend_from_slice(itoa::Buffer::new().format(count).as_bytes());
        out.extend_from_slice(b"\r\n");

        for arg in &args[1..] {
            if let RespValue::BulkString(Some(key)) = arg {
                let result = if let Some(router) = &self.router {
                    router.get(key)
                } else {
                    self.handler.get_value(key)
                };
                match result {
                    Some(value) => {
                        out.push(b'$');
                        out.extend_from_slice(itoa::Buffer::new().format(value.len()).as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&value);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => out.extend_from_slice(b"$-1\r\n"),
                }
            } else {
                out.extend_from_slice(b"$-1\r\n");
            }
        }
    }

    #[inline]
    fn cmd_mset(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            RespValue::err("MSET requires key-value pairs").serialize_into(out);
            return;
        }

        let mut i = 1;
        while i + 1 < args.len() {
            let key = match &args[i] {
                RespValue::BulkString(Some(data)) => data,
                _ => {
                    RespValue::err("Invalid key").serialize_into(out);
                    return;
                }
            };
            let value = match &args[i + 1] {
                RespValue::BulkString(Some(data)) => data,
                _ => {
                    RespValue::err("Invalid value").serialize_into(out);
                    return;
                }
            };

            let result = if let Some(router) = &self.router {
                router.put(key, value, 0)
            } else {
                self.handler.put_sync(key, value, 0)
            };

            if let Err(e) = result {
                RespValue::err(&e.to_string()).serialize_into(out);
                return;
            }
            i += 2;
        }

        RespValue::ok().serialize_into(out);
    }

    #[inline]
    fn cmd_exists(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("EXISTS requires at least 1 argument").serialize_into(out);
            return;
        }

        let mut count = 0i64;
        for arg in &args[1..] {
            if let RespValue::BulkString(Some(key)) = arg {
                let exists = if let Some(router) = &self.router {
                    router.get(key).is_some()
                } else {
                    self.handler.get_value(key).is_some()
                };
                if exists {
                    count += 1;
                }
            }
        }

        RespValue::Integer(count).serialize_into(out);
    }

    #[inline]
    fn cmd_command(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"*0\r\n");
    }

    #[inline]
    fn cmd_info(&self, out: &mut Vec<u8>) {
        let mode = if self.router.is_some() { "cluster" } else { "standalone" };
        let info = format!(
            "# Server\r\nredis_version:7.0.0-ssd-kv\r\nredis_mode:{}\r\n# Keyspace\r\n",
            mode
        );
        let info_bytes = info.as_bytes();
        out.push(b'$');
        out.extend_from_slice(itoa::Buffer::new().format(info_bytes.len()).as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(info_bytes);
        out.extend_from_slice(b"\r\n");
    }

    #[inline]
    fn cmd_dbsize(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b":0\r\n");
    }

    #[inline]
    fn cmd_cluster(&self, args: &[RespValue], out: &mut Vec<u8>) {
        let subcmd = if args.len() > 1 {
            match &args[1] {
                RespValue::BulkString(Some(data)) => data.as_slice(),
                _ => b"",
            }
        } else {
            b""
        };

        match subcmd {
            b"INFO" | b"info" => {
                if let Some(router) = &self.router {
                    let topo = router.topology().read();
                    let info = format!(
                        "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:256\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\nlocal_node_id:{}\r\ntopology_version:{}\r\n",
                        topo.nodes.len(),
                        topo.active_node_count(),
                        topo.local_node_id,
                        topo.current_version(),
                    );
                    RespValue::bulk(info.into_bytes()).serialize_into(out);
                } else {
                    let info = "cluster_enabled:0\r\n";
                    RespValue::bulk(info.as_bytes().to_vec()).serialize_into(out);
                }
            }
            b"MYID" | b"myid" => {
                if let Some(router) = &self.router {
                    let topo = router.topology().read();
                    RespValue::bulk(topo.local_node_id.to_string().into_bytes()).serialize_into(out);
                } else {
                    RespValue::bulk(b"0".to_vec()).serialize_into(out);
                }
            }
            b"NODES" | b"nodes" => {
                if let Some(router) = &self.router {
                    let topo = router.topology().read();
                    let mut nodes_info = String::new();
                    for node in &topo.nodes {
                        let shards = topo.shards_for_node(node.id);
                        nodes_info.push_str(&format!(
                            "{}  {}  {:?}  shards:{}\r\n",
                            node.id,
                            node.cluster_addr,
                            node.status,
                            shards.len(),
                        ));
                    }
                    RespValue::bulk(nodes_info.into_bytes()).serialize_into(out);
                } else {
                    RespValue::bulk(b"standalone mode".to_vec()).serialize_into(out);
                }
            }
            _ => {
                RespValue::err("Unknown CLUSTER subcommand").serialize_into(out);
            }
        }
    }
}

/// High-performance Redis server with pipelining
pub struct RedisServer {
    addr: SocketAddr,
    handler: Arc<Handler>,
    router: Option<Arc<ClusterRouter>>,
}

impl RedisServer {
    pub fn new(addr: SocketAddr, handler: Arc<Handler>) -> Self {
        Self { addr, handler, router: None }
    }

    pub fn with_router(addr: SocketAddr, handler: Arc<Handler>, router: Arc<ClusterRouter>) -> Self {
        Self { addr, handler, router: Some(router) }
    }

    pub fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(self.addr)?;
        info!("Redis server listening on {}", self.addr);

        // Set listener to non-blocking for graceful shutdown
        listener.set_nonblocking(false)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let handler = Arc::clone(&self.handler);
                    let router = self.router.clone();
                    std::thread::spawn(move || {
                        if let Err(e) = handle_redis_client_fast(stream, handler, router) {
                            debug!("Redis client error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                }
            }
        }

        Ok(())
    }
}

/// High-performance client handler with pipelining
fn handle_redis_client_fast(
    mut stream: TcpStream,
    handler: Arc<Handler>,
    router: Option<Arc<ClusterRouter>>,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(300)))?;

    let peer = stream.peer_addr()?;
    debug!("Redis client connected: {}", peer);

    let mut parser = RespParser::new();
    let redis_handler = if let Some(router) = router {
        RedisHandler::with_router(handler, router)
    } else {
        RedisHandler::new(handler)
    };
    let mut write_buf = Vec::with_capacity(WRITE_BUFFER_SIZE);
    let mut commands_in_batch = 0;

    loop {
        match parser.try_parse(&mut stream) {
            Ok(Some(value)) => {
                // Handle command and buffer response
                redis_handler.handle_command(value, &mut write_buf);
                commands_in_batch += 1;

                // Flush if buffer is large or many commands processed
                if write_buf.len() >= WRITE_BUFFER_SIZE / 2 || commands_in_batch >= MAX_PIPELINE_DEPTH {
                    stream.write_all(&write_buf)?;
                    write_buf.clear();
                    commands_in_batch = 0;
                }
            }
            Ok(None) => {
                // EOF - flush remaining and exit
                if !write_buf.is_empty() {
                    stream.write_all(&write_buf)?;
                }
                debug!("Redis client disconnected: {}", peer);
                break;
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof ||
                   e.kind() == io::ErrorKind::TimedOut ||
                   e.kind() == io::ErrorKind::ConnectionReset {
                    if !write_buf.is_empty() {
                        let _ = stream.write_all(&write_buf);
                    }
                    break;
                }
                // Write error response
                RespValue::err(&e.to_string()).serialize_into(&mut write_buf);
                stream.write_all(&write_buf)?;
                write_buf.clear();
            }
        }

        // Flush if no more data in buffer (end of pipeline)
        if parser.len == parser.pos && !write_buf.is_empty() {
            stream.write_all(&write_buf)?;
            write_buf.clear();
            commands_in_batch = 0;
        }
    }

    Ok(())
}

/// Starts the Redis server on a separate thread
pub fn start_redis_server(addr: SocketAddr, handler: Arc<Handler>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let server = RedisServer::new(addr, handler);
        if let Err(e) = server.run() {
            error!("Redis server error: {}", e);
        }
    })
}

/// Starts the Redis server with cluster routing on a separate thread
pub fn start_redis_server_clustered(
    addr: SocketAddr,
    handler: Arc<Handler>,
    router: Arc<ClusterRouter>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let server = RedisServer::with_router(addr, handler, router);
        if let Err(e) = server.run() {
            error!("Redis server error: {}", e);
        }
    })
}
