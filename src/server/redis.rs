//! High-performance Redis protocol (RESP) server.
//!
//! Optimizations:
//! - Pipelined command processing (batch multiple commands before flushing)
//! - Pre-allocated buffers for zero-copy
//! - Inline response building
//! - TCP_NODELAY for low latency

use std::cell::Cell;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{debug, error, info, trace};

use crate::cluster::router::{ClusterRouter, RouteDecision};
use crate::cluster::topology::{ClusterTopology, key_hash_slot, NUM_SLOTS};
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

/// Parsed SET command options.
#[derive(Debug, Default)]
struct SetOptions {
    ttl_secs: u32,
    nx: bool,
    xx: bool,
    keepttl: bool,
    get: bool,
}

/// High-performance Redis command handler
pub struct RedisHandler {
    pub handler: Arc<Handler>,
    pub router: Option<Arc<ClusterRouter>>,
    /// Per-connection flag: when true, read commands are served locally on replicas.
    readonly: Cell<bool>,
    /// Server-level flag: when false, READONLY is accepted but has no routing effect.
    replica_read: bool,
}

impl RedisHandler {
    pub fn new(handler: Arc<Handler>) -> Self {
        Self { handler, router: None, readonly: Cell::new(false), replica_read: false }
    }

    pub fn with_router(handler: Arc<Handler>, router: Arc<ClusterRouter>, replica_read: bool) -> Self {
        Self {
            handler,
            router: Some(router),
            readonly: Cell::new(false),
            replica_read,
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
            b"TTL" | b"ttl" => self.cmd_ttl(&args, out),
            b"PTTL" | b"pttl" => self.cmd_pttl(&args, out),
            b"EXPIRE" | b"expire" => self.cmd_expire(&args, out),
            b"PEXPIRE" | b"pexpire" => self.cmd_pexpire(&args, out),
            b"PERSIST" | b"persist" => self.cmd_persist(&args, out),
            b"EXPIREAT" | b"expireat" => self.cmd_expireat(&args, out),
            b"PEXPIREAT" | b"pexpireat" => self.cmd_pexpireat(&args, out),
            b"READONLY" | b"readonly" => self.cmd_readonly(out),
            b"READWRITE" | b"readwrite" => self.cmd_readwrite(out),
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

    /// Checks if a key should be redirected via MOVED. Returns true if MOVED was written.
    #[inline]
    fn check_moved(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        if let Some(router) = &self.router {
            let (slot, decision) = router.route_key_with_slot(key);
            if let RouteDecision::Forward(node_id) = decision {
                if let Some(addr) = router.redis_addr_for_slot(slot) {
                    let msg = format!("MOVED {} {}:{}", slot, addr.ip(), addr.port());
                    RespValue::Error(msg).serialize_into(out);
                    return true;
                }
            }
        }
        false
    }

    /// Like `check_moved` but allows local reads on replicas when READONLY is set.
    #[inline]
    fn check_moved_read(&self, key: &[u8], out: &mut Vec<u8>) -> bool {
        if self.readonly.get() && self.replica_read && self.is_local_replica_for_key(key) {
            return false;
        }
        self.check_moved(key, out)
    }

    /// Returns true if this node is a replica for the slot that `key` hashes to.
    fn is_local_replica_for_key(&self, key: &[u8]) -> bool {
        if let Some(router) = &self.router {
            let slot = ClusterTopology::slot_for_key(key);
            let topo = router.topology().read();
            topo.is_local_replica(slot)
        } else {
            false
        }
    }

    #[inline]
    fn cmd_readonly(&self, out: &mut Vec<u8>) {
        self.readonly.set(true);
        RespValue::ok().serialize_into(out);
    }

    #[inline]
    fn cmd_readwrite(&self, out: &mut Vec<u8>) {
        self.readonly.set(false);
        RespValue::ok().serialize_into(out);
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

        if self.check_moved_read(key, out) {
            return;
        }

        match self.handler.get_value(key) {
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

        if self.check_moved(key, out) {
            return;
        }

        let value = match &args[2] {
            RespValue::BulkString(Some(data)) => data,
            _ => {
                RespValue::err("Invalid value").serialize_into(out);
                return;
            }
        };

        // Parse all SET options
        let opts = match self.parse_set_options(&args[3..]) {
            Ok(o) => o,
            Err(msg) => {
                RespValue::err(&msg).serialize_into(out);
                return;
            }
        };

        // NX: only set if key does NOT exist
        if opts.nx {
            if self.handler.get_value(key).is_some() {
                RespValue::null().serialize_into(out);
                return;
            }
        }

        // XX: only set if key EXISTS
        if opts.xx {
            if self.handler.get_value(key).is_none() {
                RespValue::null().serialize_into(out);
                return;
            }
        }

        // GET: capture old value before writing
        let old_value = if opts.get {
            self.handler.get_value(key)
        } else {
            None
        };

        // Determine final TTL
        let final_ttl = if opts.keepttl {
            // Preserve existing TTL
            self.handler
                .get_with_meta(key)
                .map(|m| m.ttl_secs)
                .unwrap_or(0)
        } else {
            opts.ttl_secs
        };

        match self.handler.put_sync(key, value, final_ttl) {
            Ok(_) => {
                if opts.get {
                    match old_value {
                        Some(v) => RespValue::bulk(v).serialize_into(out),
                        None => RespValue::null().serialize_into(out),
                    }
                } else {
                    RespValue::ok().serialize_into(out);
                }
            }
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    fn parse_set_options(&self, args: &[RespValue]) -> Result<SetOptions, String> {
        let mut opts = SetOptions::default();
        let mut i = 0;
        while i < args.len() {
            if let RespValue::BulkString(Some(opt)) = &args[i] {
                match opt.as_slice() {
                    b"EX" | b"ex" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        if let RespValue::BulkString(Some(val)) = &args[i + 1] {
                            match std::str::from_utf8(val).ok().and_then(|s| s.parse::<u32>().ok()) {
                                Some(secs) if secs > 0 => {
                                    opts.ttl_secs = secs;
                                    i += 2;
                                    continue;
                                }
                                _ => return Err("value is not an integer or out of range".to_string()),
                            }
                        } else {
                            return Err("syntax error".to_string());
                        }
                    }
                    b"PX" | b"px" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        if let RespValue::BulkString(Some(val)) = &args[i + 1] {
                            match std::str::from_utf8(val).ok().and_then(|s| s.parse::<u64>().ok()) {
                                Some(ms) if ms > 0 => {
                                    opts.ttl_secs = (ms / 1000).max(1) as u32;
                                    i += 2;
                                    continue;
                                }
                                _ => return Err("value is not an integer or out of range".to_string()),
                            }
                        } else {
                            return Err("syntax error".to_string());
                        }
                    }
                    b"EXAT" | b"exat" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        if let RespValue::BulkString(Some(val)) = &args[i + 1] {
                            match std::str::from_utf8(val).ok().and_then(|s| s.parse::<u64>().ok()) {
                                Some(timestamp) => {
                                    let now_secs = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .map(|d| d.as_secs())
                                        .unwrap_or(0);
                                    if timestamp <= now_secs {
                                        return Err("invalid expire time in 'set' command".to_string());
                                    }
                                    opts.ttl_secs = (timestamp - now_secs) as u32;
                                    i += 2;
                                    continue;
                                }
                                _ => return Err("value is not an integer or out of range".to_string()),
                            }
                        } else {
                            return Err("syntax error".to_string());
                        }
                    }
                    b"PXAT" | b"pxat" => {
                        if i + 1 >= args.len() {
                            return Err("syntax error".to_string());
                        }
                        if let RespValue::BulkString(Some(val)) = &args[i + 1] {
                            match std::str::from_utf8(val).ok().and_then(|s| s.parse::<u64>().ok()) {
                                Some(timestamp_ms) => {
                                    let now_ms = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .map(|d| d.as_millis() as u64)
                                        .unwrap_or(0);
                                    if timestamp_ms <= now_ms {
                                        return Err("invalid expire time in 'set' command".to_string());
                                    }
                                    opts.ttl_secs = ((timestamp_ms - now_ms) / 1000).max(1) as u32;
                                    i += 2;
                                    continue;
                                }
                                _ => return Err("value is not an integer or out of range".to_string()),
                            }
                        } else {
                            return Err("syntax error".to_string());
                        }
                    }
                    b"NX" | b"nx" => {
                        opts.nx = true;
                        i += 1;
                        continue;
                    }
                    b"XX" | b"xx" => {
                        opts.xx = true;
                        i += 1;
                        continue;
                    }
                    b"KEEPTTL" | b"keepttl" => {
                        opts.keepttl = true;
                        i += 1;
                        continue;
                    }
                    b"GET" | b"get" => {
                        opts.get = true;
                        i += 1;
                        continue;
                    }
                    _ => {
                        return Err("syntax error".to_string());
                    }
                }
            }
            i += 1;
        }
        // NX and XX are mutually exclusive
        if opts.nx && opts.xx {
            return Err("XX and NX options at the same time are not compatible".to_string());
        }
        Ok(opts)
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
                if self.check_moved(key, out) {
                    return;
                }
                if let Ok(true) = self.handler.delete_sync(key) {
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

        // In cluster mode, verify all keys hash to the same slot
        if self.router.is_some() {
            if let Some(first_key) = args[1..].iter().find_map(|a| {
                if let RespValue::BulkString(Some(k)) = a { Some(k.as_slice()) } else { None }
            }) {
                let first_slot = key_hash_slot(first_key);
                for arg in &args[2..] {
                    if let RespValue::BulkString(Some(k)) = arg {
                        if key_hash_slot(k) != first_slot {
                            RespValue::Error("CROSSSLOT Keys in request don't hash to the same slot".to_string())
                                .serialize_into(out);
                            return;
                        }
                    }
                }
                if self.check_moved_read(first_key, out) {
                    return;
                }
            }
        }

        let count = args.len() - 1;
        out.push(b'*');
        out.extend_from_slice(itoa::Buffer::new().format(count).as_bytes());
        out.extend_from_slice(b"\r\n");

        for arg in &args[1..] {
            if let RespValue::BulkString(Some(key)) = arg {
                match self.handler.get_value(key) {
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

        // In cluster mode, verify all keys hash to the same slot
        if self.router.is_some() {
            let mut first_slot = None;
            let mut i = 1;
            while i + 1 < args.len() {
                if let RespValue::BulkString(Some(k)) = &args[i] {
                    let slot = key_hash_slot(k);
                    if let Some(fs) = first_slot {
                        if slot != fs {
                            RespValue::Error("CROSSSLOT Keys in request don't hash to the same slot".to_string())
                                .serialize_into(out);
                            return;
                        }
                    } else {
                        first_slot = Some(slot);
                        if self.check_moved(k, out) {
                            return;
                        }
                    }
                }
                i += 2;
            }
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

            if let Err(e) = self.handler.put_sync(key, value, 0) {
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
                if self.check_moved_read(key, out) {
                    return;
                }
                if self.handler.get_value(key).is_some() {
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
                    let total_slots = NUM_SLOTS;
                    let mut slots_ok = 0usize;
                    let mut slots_pfail = 0usize;
                    let mut slots_fail = 0usize;
                    for assignment in &topo.shard_map {
                        if let Some(node) = topo.get_node(assignment.primary) {
                            match node.status {
                                crate::cluster::node::NodeStatus::Active => slots_ok += 1,
                                crate::cluster::node::NodeStatus::Suspect => slots_pfail += 1,
                                crate::cluster::node::NodeStatus::Dead => slots_fail += 1,
                                _ => slots_ok += 1,
                            }
                        }
                    }
                    let info = format!(
                        "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:{}\r\ncluster_slots_ok:{}\r\ncluster_slots_pfail:{}\r\ncluster_slots_fail:{}\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\nlocal_node_id:{}\r\ntopology_version:{}\r\n",
                        total_slots,
                        slots_ok,
                        slots_pfail,
                        slots_fail,
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
                    RespValue::bulk(format!("{:040x}", topo.local_node_id).into_bytes()).serialize_into(out);
                } else {
                    RespValue::bulk(format!("{:040x}", 0u32).into_bytes()).serialize_into(out);
                }
            }
            b"NODES" | b"nodes" => {
                if let Some(router) = &self.router {
                    let topo = router.topology().read();
                    let mut nodes_info = String::new();
                    for node in &topo.nodes {
                        let node_id_hex = format!("{:040x}", node.id);
                        let flags = if node.id == topo.local_node_id {
                            match node.status {
                                crate::cluster::node::NodeStatus::Active => "myself,master",
                                crate::cluster::node::NodeStatus::Suspect => "myself,master,fail?",
                                crate::cluster::node::NodeStatus::Dead => "myself,master,fail",
                                _ => "myself,master",
                            }
                        } else {
                            match node.status {
                                crate::cluster::node::NodeStatus::Active => "master",
                                crate::cluster::node::NodeStatus::Suspect => "master,fail?",
                                crate::cluster::node::NodeStatus::Dead => "master,fail",
                                _ => "master",
                            }
                        };
                        let ranges = topo.slot_ranges_for_node(node.id);
                        let slot_str: Vec<String> = ranges
                            .iter()
                            .map(|(s, e)| {
                                if s == e { format!("{}", s) } else { format!("{}-{}", s, e) }
                            })
                            .collect();
                        nodes_info.push_str(&format!(
                            "{} {}:{}@{} {} - 0 0 {} connected {}\n",
                            node_id_hex,
                            node.redis_addr.ip(),
                            node.redis_addr.port(),
                            node.cluster_addr.port(),
                            flags,
                            topo.current_version(),
                            slot_str.join(" "),
                        ));
                    }
                    RespValue::bulk(nodes_info.into_bytes()).serialize_into(out);
                } else {
                    RespValue::bulk(b"standalone mode".to_vec()).serialize_into(out);
                }
            }
            b"SLOTS" | b"slots" => {
                if let Some(router) = &self.router {
                    let topo = router.topology().read();
                    // Build slot ranges per node, then generate RESP
                    let mut all_ranges = Vec::new();
                    for node in &topo.nodes {
                        let ranges = topo.slot_ranges_for_node(node.id);
                        for (start, end) in ranges {
                            // Collect primary + replicas for these slots
                            let assignment = &topo.shard_map[start as usize];
                            let mut node_entries = Vec::new();
                            // Primary
                            if let Some(primary) = topo.get_node(assignment.primary) {
                                node_entries.push(RespValue::Array(Some(vec![
                                    RespValue::BulkString(Some(primary.redis_addr.ip().to_string().into_bytes())),
                                    RespValue::Integer(primary.redis_addr.port() as i64),
                                    RespValue::BulkString(Some(format!("{:040x}", primary.id).into_bytes())),
                                ])));
                            }
                            // Replicas
                            for &replica_id in &assignment.replicas {
                                if let Some(replica) = topo.get_node(replica_id) {
                                    node_entries.push(RespValue::Array(Some(vec![
                                        RespValue::BulkString(Some(replica.redis_addr.ip().to_string().into_bytes())),
                                        RespValue::Integer(replica.redis_addr.port() as i64),
                                        RespValue::BulkString(Some(format!("{:040x}", replica.id).into_bytes())),
                                    ])));
                                }
                            }
                            let mut entry = vec![
                                RespValue::Integer(start as i64),
                                RespValue::Integer(end as i64),
                            ];
                            entry.extend(node_entries);
                            all_ranges.push(RespValue::Array(Some(entry)));
                        }
                    }
                    RespValue::Array(Some(all_ranges)).serialize_into(out);
                } else {
                    RespValue::Array(Some(Vec::new())).serialize_into(out);
                }
            }
            b"KEYSLOT" | b"keyslot" => {
                if args.len() < 3 {
                    RespValue::err("CLUSTER KEYSLOT requires 1 argument").serialize_into(out);
                    return;
                }
                if let RespValue::BulkString(Some(key)) = &args[2] {
                    RespValue::Integer(key_hash_slot(key) as i64).serialize_into(out);
                } else {
                    RespValue::err("Invalid key").serialize_into(out);
                }
            }
            _ => {
                RespValue::err("Unknown CLUSTER subcommand").serialize_into(out);
            }
        }
    }

    fn extract_key<'a>(&self, args: &'a [RespValue], cmd_name: &str) -> Result<&'a [u8], ()> {
        if args.len() < 2 {
            return Err(());
        }
        match &args[1] {
            RespValue::BulkString(Some(data)) => Ok(data.as_slice()),
            _ => Err(()),
        }
    }

    #[inline]
    fn cmd_ttl(&self, args: &[RespValue], out: &mut Vec<u8>) {
        let key = match self.extract_key(args, "TTL") {
            Ok(k) => k,
            Err(_) => {
                RespValue::err("TTL requires 1 argument").serialize_into(out);
                return;
            }
        };

        if self.check_moved_read(key, out) {
            return;
        }

        match self.handler.get_with_meta(key) {
            None => RespValue::Integer(-2).serialize_into(out),
            Some(meta) => {
                if meta.ttl_secs == 0 {
                    RespValue::Integer(-1).serialize_into(out);
                } else {
                    let expiry_micros = meta.timestamp_micros + (meta.ttl_secs as u64 * 1_000_000);
                    let now_micros = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_micros() as u64)
                        .unwrap_or(0);
                    if now_micros >= expiry_micros {
                        RespValue::Integer(-2).serialize_into(out);
                    } else {
                        let remaining_secs = ((expiry_micros - now_micros) / 1_000_000) as i64;
                        RespValue::Integer(remaining_secs).serialize_into(out);
                    }
                }
            }
        }
    }

    #[inline]
    fn cmd_pttl(&self, args: &[RespValue], out: &mut Vec<u8>) {
        let key = match self.extract_key(args, "PTTL") {
            Ok(k) => k,
            Err(_) => {
                RespValue::err("PTTL requires 1 argument").serialize_into(out);
                return;
            }
        };

        if self.check_moved_read(key, out) {
            return;
        }

        match self.handler.get_with_meta(key) {
            None => RespValue::Integer(-2).serialize_into(out),
            Some(meta) => {
                if meta.ttl_secs == 0 {
                    RespValue::Integer(-1).serialize_into(out);
                } else {
                    let expiry_micros = meta.timestamp_micros + (meta.ttl_secs as u64 * 1_000_000);
                    let now_micros = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_micros() as u64)
                        .unwrap_or(0);
                    if now_micros >= expiry_micros {
                        RespValue::Integer(-2).serialize_into(out);
                    } else {
                        let remaining_ms = ((expiry_micros - now_micros) / 1_000) as i64;
                        RespValue::Integer(remaining_ms).serialize_into(out);
                    }
                }
            }
        }
    }

    #[inline]
    fn cmd_expire(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("EXPIRE requires 2 arguments").serialize_into(out);
            return;
        }

        let key = match &args[1] {
            RespValue::BulkString(Some(data)) => data.as_slice(),
            _ => {
                RespValue::err("Invalid key").serialize_into(out);
                return;
            }
        };

        if self.check_moved(key, out) {
            return;
        }

        let secs = match &args[2] {
            RespValue::BulkString(Some(v)) => {
                match std::str::from_utf8(v).ok().and_then(|s| s.parse::<u32>().ok()) {
                    Some(s) => s,
                    None => {
                        RespValue::err("value is not an integer or out of range").serialize_into(out);
                        return;
                    }
                }
            }
            _ => {
                RespValue::err("value is not an integer or out of range").serialize_into(out);
                return;
            }
        };

        match self.handler.update_ttl(key, secs) {
            Ok(true) => RespValue::Integer(1).serialize_into(out),
            Ok(false) => RespValue::Integer(0).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    #[inline]
    fn cmd_pexpire(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("PEXPIRE requires 2 arguments").serialize_into(out);
            return;
        }

        let key = match &args[1] {
            RespValue::BulkString(Some(data)) => data.as_slice(),
            _ => {
                RespValue::err("Invalid key").serialize_into(out);
                return;
            }
        };

        if self.check_moved(key, out) {
            return;
        }

        let ms = match &args[2] {
            RespValue::BulkString(Some(v)) => {
                match std::str::from_utf8(v).ok().and_then(|s| s.parse::<u64>().ok()) {
                    Some(m) => m,
                    None => {
                        RespValue::err("value is not an integer or out of range").serialize_into(out);
                        return;
                    }
                }
            }
            _ => {
                RespValue::err("value is not an integer or out of range").serialize_into(out);
                return;
            }
        };

        let secs = (ms / 1000) as u32;
        match self.handler.update_ttl(key, secs) {
            Ok(true) => RespValue::Integer(1).serialize_into(out),
            Ok(false) => RespValue::Integer(0).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    #[inline]
    fn cmd_persist(&self, args: &[RespValue], out: &mut Vec<u8>) {
        let key = match self.extract_key(args, "PERSIST") {
            Ok(k) => k,
            Err(_) => {
                RespValue::err("PERSIST requires 1 argument").serialize_into(out);
                return;
            }
        };

        if self.check_moved(key, out) {
            return;
        }

        // Check if key has a TTL first
        match self.handler.get_with_meta(key) {
            None => RespValue::Integer(0).serialize_into(out),
            Some(meta) => {
                if meta.ttl_secs == 0 {
                    // No TTL to remove
                    RespValue::Integer(0).serialize_into(out);
                } else {
                    match self.handler.update_ttl(key, 0) {
                        Ok(true) => RespValue::Integer(1).serialize_into(out),
                        Ok(false) => RespValue::Integer(0).serialize_into(out),
                        Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
                    }
                }
            }
        }
    }

    #[inline]
    fn cmd_expireat(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("EXPIREAT requires 2 arguments").serialize_into(out);
            return;
        }

        let key = match &args[1] {
            RespValue::BulkString(Some(data)) => data.as_slice(),
            _ => {
                RespValue::err("Invalid key").serialize_into(out);
                return;
            }
        };

        if self.check_moved(key, out) {
            return;
        }

        let timestamp = match &args[2] {
            RespValue::BulkString(Some(v)) => {
                match std::str::from_utf8(v).ok().and_then(|s| s.parse::<u64>().ok()) {
                    Some(t) => t,
                    None => {
                        RespValue::err("value is not an integer or out of range").serialize_into(out);
                        return;
                    }
                }
            }
            _ => {
                RespValue::err("value is not an integer or out of range").serialize_into(out);
                return;
            }
        };

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        if timestamp <= now_secs {
            // Already expired - delete
            let _ = self.handler.delete_sync(key);
            RespValue::Integer(1).serialize_into(out);
            return;
        }

        let remaining = (timestamp - now_secs) as u32;
        match self.handler.update_ttl(key, remaining) {
            Ok(true) => RespValue::Integer(1).serialize_into(out),
            Ok(false) => RespValue::Integer(0).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    #[inline]
    fn cmd_pexpireat(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("PEXPIREAT requires 2 arguments").serialize_into(out);
            return;
        }

        let key = match &args[1] {
            RespValue::BulkString(Some(data)) => data.as_slice(),
            _ => {
                RespValue::err("Invalid key").serialize_into(out);
                return;
            }
        };

        if self.check_moved(key, out) {
            return;
        }

        let timestamp_ms = match &args[2] {
            RespValue::BulkString(Some(v)) => {
                match std::str::from_utf8(v).ok().and_then(|s| s.parse::<u64>().ok()) {
                    Some(t) => t,
                    None => {
                        RespValue::err("value is not an integer or out of range").serialize_into(out);
                        return;
                    }
                }
            }
            _ => {
                RespValue::err("value is not an integer or out of range").serialize_into(out);
                return;
            }
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        if timestamp_ms <= now_ms {
            let _ = self.handler.delete_sync(key);
            RespValue::Integer(1).serialize_into(out);
            return;
        }

        let remaining_secs = ((timestamp_ms - now_ms) / 1000) as u32;
        match self.handler.update_ttl(key, remaining_secs) {
            Ok(true) => RespValue::Integer(1).serialize_into(out),
            Ok(false) => RespValue::Integer(0).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }
}

/// High-performance Redis server with pipelining
pub struct RedisServer {
    addr: SocketAddr,
    handler: Arc<Handler>,
    router: Option<Arc<ClusterRouter>>,
    replica_read: bool,
}

impl RedisServer {
    pub fn new(addr: SocketAddr, handler: Arc<Handler>) -> Self {
        Self { addr, handler, router: None, replica_read: false }
    }

    pub fn with_router(addr: SocketAddr, handler: Arc<Handler>, router: Arc<ClusterRouter>, replica_read: bool) -> Self {
        Self { addr, handler, router: Some(router), replica_read }
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
                    let replica_read = self.replica_read;
                    std::thread::spawn(move || {
                        if let Err(e) = handle_redis_client_fast(stream, handler, router, replica_read) {
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
    replica_read: bool,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(300)))?;

    let peer = stream.peer_addr()?;
    debug!("Redis client connected: {}", peer);

    let mut parser = RespParser::new();
    let redis_handler = if let Some(router) = router {
        RedisHandler::with_router(handler, router, replica_read)
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
    replica_read: bool,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let server = RedisServer::with_router(addr, handler, router, replica_read);
        if let Err(e) = server.run() {
            error!("Redis server error: {}", e);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node::NodeInfo;
    use crate::cluster::peer_pool::PeerConnectionPool;
    use crate::cluster::topology::ClusterTopology;
    use crate::engine::index::Index;
    use crate::storage::file_manager::FileManager;
    use crate::storage::write_buffer::WriteBuffer;
    use parking_lot::RwLock;

    fn make_handler(dir: &std::path::Path) -> Arc<Handler> {
        let fm = Arc::new(FileManager::new(dir).unwrap());
        fm.create_file().unwrap();
        let index = Arc::new(Index::new());
        let wb = Arc::new(WriteBuffer::new(0, 1023));
        Arc::new(Handler::new(index, fm, wb))
    }

    fn make_cmd(parts: &[&[u8]]) -> RespValue {
        RespValue::Array(Some(
            parts.iter().map(|p| RespValue::BulkString(Some(p.to_vec()))).collect()
        ))
    }

    #[test]
    fn test_readonly_command_responds_ok() {
        let dir = tempfile::tempdir().unwrap();
        let handler = make_handler(dir.path());
        let rh = RedisHandler::new(handler);
        let mut out = Vec::new();

        rh.handle_command(make_cmd(&[b"READONLY"]), &mut out);
        assert_eq!(out, b"+OK\r\n");
        assert!(rh.readonly.get());
    }

    #[test]
    fn test_readwrite_command_responds_ok() {
        let dir = tempfile::tempdir().unwrap();
        let handler = make_handler(dir.path());
        let rh = RedisHandler::new(handler);
        let mut out = Vec::new();

        // Set readonly first
        rh.readonly.set(true);
        rh.handle_command(make_cmd(&[b"READWRITE"]), &mut out);
        assert_eq!(out, b"+OK\r\n");
        assert!(!rh.readonly.get());
    }

    #[test]
    fn test_readonly_flag_allows_replica_reads() {
        let dir = tempfile::tempdir().unwrap();
        let handler = make_handler(dir.path());

        // Set up a 3-node cluster where this node is node 1 (not primary for all slots)
        let nodes = vec![
            NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap()),
            NodeInfo::new(1, "127.0.0.1:7778".parse().unwrap(), "127.0.0.1:7781".parse().unwrap()),
            NodeInfo::new(2, "127.0.0.1:7779".parse().unwrap(), "127.0.0.1:7782".parse().unwrap()),
        ];
        // This node is node 1, replication factor 2 means it's replica for node 0's slots
        let topo = Arc::new(RwLock::new(ClusterTopology::new(1, nodes, 2)));
        let peers = Arc::new(PeerConnectionPool::new());
        let router = Arc::new(ClusterRouter::new(topo, Arc::clone(&handler), peers));
        let rh = RedisHandler::with_router(handler, router, true);

        // Write a key locally that hashes to a slot where node 1 is a replica (node 0 is primary)
        let mut replica_key = None;
        for i in 0..10_000 {
            let key = format!("rr_key_{}", i);
            if rh.is_local_replica_for_key(key.as_bytes()) {
                replica_key = Some(key);
                break;
            }
        }
        let key = replica_key.expect("Should find a key where node 1 is replica");

        // Write it directly to the local handler (simulating replication)
        rh.handler.put_sync(key.as_bytes(), b"replica_val", 0).unwrap();

        // Without READONLY, GET should return MOVED
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"GET", key.as_bytes()]), &mut out);
        let response = String::from_utf8_lossy(&out);
        assert!(response.starts_with("-MOVED"), "Expected MOVED, got: {}", response);

        // With READONLY, GET should succeed locally
        rh.readonly.set(true);
        let mut out2 = Vec::new();
        rh.handle_command(make_cmd(&[b"GET", key.as_bytes()]), &mut out2);
        let response2 = String::from_utf8_lossy(&out2);
        assert!(!response2.starts_with("-MOVED"), "Expected local read, got: {}", response2);
        assert!(response2.contains("replica_val"));
    }

    #[test]
    fn test_readonly_writes_still_moved() {
        let dir = tempfile::tempdir().unwrap();
        let handler = make_handler(dir.path());

        let nodes = vec![
            NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap()),
            NodeInfo::new(1, "127.0.0.1:7778".parse().unwrap(), "127.0.0.1:7781".parse().unwrap()),
            NodeInfo::new(2, "127.0.0.1:7779".parse().unwrap(), "127.0.0.1:7782".parse().unwrap()),
        ];
        let topo = Arc::new(RwLock::new(ClusterTopology::new(1, nodes, 2)));
        let peers = Arc::new(PeerConnectionPool::new());
        let router = Arc::new(ClusterRouter::new(topo, Arc::clone(&handler), peers));
        let rh = RedisHandler::with_router(handler, router, true);
        rh.readonly.set(true);

        // Find a key where node 1 is replica (node 0 is primary)
        let mut replica_key = None;
        for i in 0..10_000 {
            let key = format!("wr_key_{}", i);
            if rh.is_local_replica_for_key(key.as_bytes()) {
                replica_key = Some(key);
                break;
            }
        }
        let key = replica_key.expect("Should find replica key");

        // SET should still return MOVED even with READONLY
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", key.as_bytes(), b"val"]), &mut out);
        let response = String::from_utf8_lossy(&out);
        assert!(response.starts_with("-MOVED"), "Writes should MOVED even with READONLY: {}", response);
    }

    #[test]
    fn test_readonly_without_replica_read_config() {
        let dir = tempfile::tempdir().unwrap();
        let handler = make_handler(dir.path());

        let nodes = vec![
            NodeInfo::new(0, "127.0.0.1:7777".parse().unwrap(), "127.0.0.1:7780".parse().unwrap()),
            NodeInfo::new(1, "127.0.0.1:7778".parse().unwrap(), "127.0.0.1:7781".parse().unwrap()),
        ];
        let topo = Arc::new(RwLock::new(ClusterTopology::new(1, nodes, 2)));
        let peers = Arc::new(PeerConnectionPool::new());
        let router = Arc::new(ClusterRouter::new(topo, Arc::clone(&handler), peers));
        // replica_read = false (server disabled)
        let rh = RedisHandler::with_router(handler, router, false);
        rh.readonly.set(true);

        // Find a key where we're replica
        let mut replica_key = None;
        for i in 0..10_000 {
            let key = format!("nr_key_{}", i);
            if rh.is_local_replica_for_key(key.as_bytes()) {
                replica_key = Some(key);
                break;
            }
        }
        let key = replica_key.expect("Should find replica key");

        // Even with READONLY set, should still MOVED because replica_read config is false
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"GET", key.as_bytes()]), &mut out);
        let response = String::from_utf8_lossy(&out);
        assert!(response.starts_with("-MOVED"), "Expected MOVED when replica_read=false: {}", response);
    }
}
