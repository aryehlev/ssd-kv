//! High-performance Redis protocol (RESP) server.
//!
//! Optimizations:
//! - Pipelined command processing (batch multiple commands before flushing)
//! - Pre-allocated buffers for zero-copy
//! - Inline response building
//! - TCP_NODELAY for low latency

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{debug, error, info, trace};

use crate::cluster::router::{ClusterRouter, RouteDecision};
use crate::cluster::topology::{ClusterTopology, key_hash_slot, NUM_SLOTS};
use crate::server::db_manager::{DatabaseManager, DbHandler};
use crate::server::handler::Handler;

/// Maximum pipeline depth before forcing flush
const MAX_PIPELINE_DEPTH: usize = 128;

/// Maximum read buffer size (64MB - supports arbitrarily large values)
const MAX_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// Per-server tuning sizes derived from CLI flags.
#[derive(Clone, Copy, Debug)]
pub struct ServerTuning {
    pub read_buf_bytes: usize,
    pub write_buf_bytes: usize,
    pub max_connections: usize,
}

impl ServerTuning {
    /// Defaults used by tests and anywhere a Config isn't handy.
    pub const fn default_test() -> Self {
        Self {
            read_buf_bytes: 64 * 1024,
            write_buf_bytes: 64 * 1024,
            max_connections: 10_000,
        }
    }
}

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
    /// Initial buffer size; the floor that `maybe_shrink` targets.
    initial: usize,
}

impl RespParser {
    pub fn new(initial: usize) -> Self {
        Self {
            buf: vec![0u8; initial],
            pos: 0,
            len: 0,
            initial,
        }
    }

    /// Read more data from the stream, growing the buffer if needed
    #[inline]
    fn fill_buffer(&mut self, stream: &mut impl Read) -> io::Result<bool> {
        let remaining_space = self.buf.len() - self.len;

        // Only compact when less than 25% of buffer is free, to reduce memcpy frequency
        if remaining_space < self.buf.len() / 4 {
            if self.pos > 0 {
                if self.pos < self.len {
                    self.buf.copy_within(self.pos..self.len, 0);
                    self.len -= self.pos;
                } else {
                    self.len = 0;
                }
                self.pos = 0;
            }

            // Grow buffer if still full after compaction
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
        if self.buf.len() > self.initial && remaining < self.buf.len() / 4 {
            let new_size = (self.buf.len() / 2).max(self.initial);
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
                // Only shrink occasionally — checking buffer size is cheaper than shrink logic
                if self.buf.len() > self.initial {
                    self.maybe_shrink();
                }
                return Ok(Some(value));
            }

            // Need more data
            if !self.fill_buffer(stream)? {
                return Ok(None); // EOF
            }
        }
    }

    /// Try to parse a simple GET or SET command directly from the buffer
    /// without allocating any Vec<u8>. Returns true if a fast command was handled.
    /// This avoids all heap allocations for the most common commands.
    #[inline]
    fn try_fast_command(
        &mut self,
        handler: &DbHandler,
        out: &mut Vec<u8>,
    ) -> io::Result<Option<bool>> {
        if self.pos >= self.len {
            return Ok(None);
        }
        let start = self.pos;
        let buf = &self.buf[..self.len];

        // Must start with *2 or *3 (GET has 2 args, SET has 3)
        if buf[start] != b'*' {
            return Ok(Some(false)); // not an array, fall through
        }

        // Parse array count
        let (argc, mut cursor) = match Self::parse_inline_int(buf, start + 1) {
            Some(v) => v,
            None => return Ok(None), // need more data
        };

        if argc != 2 && argc != 3 {
            return Ok(Some(false)); // not GET/SET, fall through
        }

        // Parse command name bulk string
        let (cmd_start, cmd_len, next) = match Self::parse_inline_bulk(buf, cursor) {
            Some(v) => v,
            None => return Ok(None),
        };
        cursor = next;

        if cmd_len != 3 {
            return Ok(Some(false)); // not GET/SET
        }

        let cmd = &buf[cmd_start..cmd_start + 3];

        if argc == 2 && (cmd == b"GET" || cmd == b"get") {
            // Parse key
            let (key_start, key_len, next) = match Self::parse_inline_bulk(buf, cursor) {
                Some(v) => v,
                None => return Ok(None),
            };
            let key = &buf[key_start..key_start + key_len];
            self.pos = next;

            // Zero-copy GET: write directly from DashMap guard
            if !handler.get_value_into(key, out) {
                out.extend_from_slice(b"$-1\r\n");
            }
            return Ok(Some(true));
        }

        if argc == 3 && (cmd == b"SET" || cmd == b"set") {
            // Parse key
            let (key_start, key_len, next) = match Self::parse_inline_bulk(buf, cursor) {
                Some(v) => v,
                None => return Ok(None),
            };
            cursor = next;

            // Parse value
            let (val_start, val_len, next) = match Self::parse_inline_bulk(buf, cursor) {
                Some(v) => v,
                None => return Ok(None),
            };

            let key = &buf[key_start..key_start + key_len];
            let value = &buf[val_start..val_start + val_len];
            self.pos = next;

            match handler.put_sync(key, value, 0) {
                Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
            }
            return Ok(Some(true));
        }

        Ok(Some(false)) // not GET/SET
    }

    /// Parse a \r\n-terminated integer from buffer. Returns (value, pos_after_crlf).
    #[inline]
    fn parse_inline_int(buf: &[u8], start: usize) -> Option<(usize, usize)> {
        let mut i = start;
        while i < buf.len() {
            if buf[i] == b'\r' {
                if i + 1 >= buf.len() {
                    return None;
                }
                // Parse the number from start..i
                let mut val = 0usize;
                for &b in &buf[start..i] {
                    if b < b'0' || b > b'9' {
                        return None; // not a simple positive integer
                    }
                    val = val * 10 + (b - b'0') as usize;
                }
                return Some((val, i + 2)); // skip \r\n
            }
            i += 1;
        }
        None
    }

    /// Parse a RESP bulk string: $len\r\n<data>\r\n
    /// Returns (data_start, data_len, pos_after_final_crlf).
    #[inline]
    fn parse_inline_bulk(buf: &[u8], start: usize) -> Option<(usize, usize, usize)> {
        if start >= buf.len() || buf[start] != b'$' {
            return None;
        }
        let (len, data_start) = Self::parse_inline_int(buf, start + 1)?;
        let data_end = data_start + len;
        if data_end + 2 > buf.len() {
            return None; // need more data
        }
        Some((data_start, len, data_end + 2)) // skip trailing \r\n
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
        // SIMD-accelerated CRLF scan. memchr::memchr uses AVX2/NEON to hunt
        // for '\n' ~16× faster than a scalar byte loop, which matters when
        // requests come in big pipelined batches.
        let start = self.pos;
        match memchr::memchr(b'\n', &self.buf[start..self.len]) {
            Some(rel) => {
                let lf = start + rel;
                // Standard RESP requires CRLF; bare LF is malformed but we
                // accept it defensively (no leading-\r check means the
                // line slice is just up to LF, not LF-1).
                let end = if lf > start && self.buf[lf - 1] == b'\r' {
                    lf - 1
                } else {
                    lf
                };
                self.pos = lf + 1;
                Ok(Some(&self.buf[start..end]))
            }
            None => Ok(None),
        }
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

/// Shared pub/sub manager for cross-connection messaging.
pub struct PubSubManager {
    /// channel -> set of subscriber sender handles
    channels: parking_lot::RwLock<HashMap<Vec<u8>, Vec<std::sync::mpsc::Sender<PubSubMessage>>>>,
    /// pattern -> set of subscriber sender handles
    patterns: parking_lot::RwLock<HashMap<Vec<u8>, Vec<std::sync::mpsc::Sender<PubSubMessage>>>>,
}

/// A message delivered to a subscriber.
#[derive(Clone, Debug)]
pub struct PubSubMessage {
    pub kind: String,        // "message" or "pmessage"
    pub channel: Vec<u8>,
    pub pattern: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

impl PubSubManager {
    pub fn new() -> Self {
        Self {
            channels: parking_lot::RwLock::new(HashMap::new()),
            patterns: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn subscribe(&self, channel: &[u8], sender: std::sync::mpsc::Sender<PubSubMessage>) {
        let mut channels = self.channels.write();
        channels.entry(channel.to_vec()).or_default().push(sender);
    }

    pub fn unsubscribe(&self, channel: &[u8], sender_id: usize) {
        let mut channels = self.channels.write();
        if let Some(senders) = channels.get_mut(channel) {
            senders.retain(|s| {
                // Remove disconnected senders
                s.send(PubSubMessage {
                    kind: String::new(),
                    channel: Vec::new(),
                    pattern: None,
                    data: Vec::new(),
                }).is_ok()
            });
        }
    }

    pub fn psubscribe(&self, pattern: &[u8], sender: std::sync::mpsc::Sender<PubSubMessage>) {
        let mut patterns = self.patterns.write();
        patterns.entry(pattern.to_vec()).or_default().push(sender);
    }

    pub fn publish(&self, channel: &[u8], message: &[u8]) -> i64 {
        let mut count = 0i64;

        // Direct channel subscribers
        let channels = self.channels.read();
        if let Some(senders) = channels.get(channel) {
            for sender in senders {
                let msg = PubSubMessage {
                    kind: "message".to_string(),
                    channel: channel.to_vec(),
                    pattern: None,
                    data: message.to_vec(),
                };
                if sender.send(msg).is_ok() {
                    count += 1;
                }
            }
        }
        drop(channels);

        // Pattern subscribers
        let patterns = self.patterns.read();
        for (pattern, senders) in patterns.iter() {
            if glob_match(pattern, channel) {
                for sender in senders {
                    let msg = PubSubMessage {
                        kind: "pmessage".to_string(),
                        channel: channel.to_vec(),
                        pattern: Some(pattern.clone()),
                        data: message.to_vec(),
                    };
                    if sender.send(msg).is_ok() {
                        count += 1;
                    }
                }
            }
        }

        count
    }
}

/// High-performance Redis command handler
pub struct RedisHandler {
    pub db_manager: Arc<DatabaseManager>,
    pub router: Option<Arc<ClusterRouter>>,
    /// Per-connection flag: when true, read commands are served locally on replicas.
    readonly: Cell<bool>,
    /// Server-level flag: when false, READONLY is accepted but has no routing effect.
    replica_read: bool,
    /// Current database index (SELECT changes this).
    current_db: Cell<u8>,
    /// Shared pub/sub manager.
    pub pubsub: Arc<PubSubManager>,
    /// Transaction state: None = not in transaction, Some = queued commands.
    tx_queue: std::cell::RefCell<Option<Vec<Vec<RespValue>>>>,
    /// Watched keys for optimistic locking (key -> generation at watch time).
    watched_keys: std::cell::RefCell<HashMap<Vec<u8>, Option<u32>>>,
}

impl RedisHandler {
    pub fn new(db_manager: Arc<DatabaseManager>) -> Self {
        Self {
            db_manager,
            router: None,
            readonly: Cell::new(false),
            replica_read: false,
            current_db: Cell::new(0),
            pubsub: Arc::new(PubSubManager::new()),
            tx_queue: std::cell::RefCell::new(None),
            watched_keys: std::cell::RefCell::new(HashMap::new()),
        }
    }

    pub fn new_with_pubsub(db_manager: Arc<DatabaseManager>, pubsub: Arc<PubSubManager>) -> Self {
        Self {
            db_manager,
            router: None,
            readonly: Cell::new(false),
            replica_read: false,
            current_db: Cell::new(0),
            pubsub,
            tx_queue: std::cell::RefCell::new(None),
            watched_keys: std::cell::RefCell::new(HashMap::new()),
        }
    }

    pub fn with_router(db_manager: Arc<DatabaseManager>, router: Arc<ClusterRouter>, replica_read: bool) -> Self {
        Self {
            db_manager,
            router: Some(router),
            readonly: Cell::new(false),
            replica_read,
            current_db: Cell::new(0),
            pubsub: Arc::new(PubSubManager::new()),
            tx_queue: std::cell::RefCell::new(None),
            watched_keys: std::cell::RefCell::new(HashMap::new()),
        }
    }

    pub fn with_router_and_pubsub(
        db_manager: Arc<DatabaseManager>,
        router: Arc<ClusterRouter>,
        replica_read: bool,
        pubsub: Arc<PubSubManager>,
    ) -> Self {
        Self {
            db_manager,
            router: Some(router),
            readonly: Cell::new(false),
            replica_read,
            current_db: Cell::new(0),
            pubsub,
            tx_queue: std::cell::RefCell::new(None),
            watched_keys: std::cell::RefCell::new(HashMap::new()),
        }
    }

    /// Returns the current database handler.
    #[inline]
    fn current_handler(&self) -> &DbHandler {
        self.db_manager.db(self.current_db.get()).unwrap()
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

        // Validate command format before transaction check
        if !matches!(&args[0], RespValue::BulkString(Some(_))) {
            RespValue::err("Invalid command format").serialize_into(out);
            return;
        }

        // If in MULTI transaction mode, queue commands (except EXEC, DISCARD, MULTI)
        {
            let tx = self.tx_queue.borrow();
            if tx.is_some() {
                let is_tx_cmd = if let RespValue::BulkString(Some(data)) = &args[0] {
                    matches!(data.as_slice(),
                        b"EXEC" | b"exec" | b"DISCARD" | b"discard" | b"MULTI" | b"multi"
                    )
                } else {
                    false
                };
                if !is_tx_cmd {
                    drop(tx);
                    self.tx_queue.borrow_mut().as_mut().unwrap().push(args);
                    out.extend_from_slice(b"+QUEUED\r\n");
                    return;
                }
            }
        }

        // Borrow command name without cloning (borrow is safe since args isn't moved below)
        let cmd = match &args[0] {
            RespValue::BulkString(Some(data)) => data.as_slice(),
            _ => unreachable!(), // validated above
        };

        // Fast path for common commands
        match cmd {
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
            b"ECHO" | b"echo" => self.cmd_echo(&args, out),
            b"TIME" | b"time" => self.cmd_time(out),
            b"TYPE" | b"type" => self.cmd_type(&args, out),
            b"UNLINK" | b"unlink" => self.cmd_del(&args, out),
            b"FLUSHDB" | b"flushdb" => self.cmd_flushdb(out),
            b"FLUSHALL" | b"flushall" => self.cmd_flushdb(out),
            b"INCR" | b"incr" => self.cmd_incr(&args, out),
            b"DECR" | b"decr" => self.cmd_decr(&args, out),
            b"INCRBY" | b"incrby" => self.cmd_incrby(&args, out),
            b"DECRBY" | b"decrby" => self.cmd_decrby(&args, out),
            b"INCRBYFLOAT" | b"incrbyfloat" => self.cmd_incrbyfloat(&args, out),
            b"APPEND" | b"append" => self.cmd_append(&args, out),
            b"STRLEN" | b"strlen" => self.cmd_strlen(&args, out),
            b"GETRANGE" | b"getrange" => self.cmd_getrange(&args, out),
            b"SETRANGE" | b"setrange" => self.cmd_setrange(&args, out),
            b"SETNX" | b"setnx" => self.cmd_setnx(&args, out),
            b"SETEX" | b"setex" => self.cmd_setex(&args, out),
            b"PSETEX" | b"psetex" => self.cmd_psetex(&args, out),
            b"GETDEL" | b"getdel" => self.cmd_getdel(&args, out),
            b"GETEX" | b"getex" => self.cmd_getex(&args, out),
            b"KEYS" | b"keys" => self.cmd_keys(&args, out),
            b"SCAN" | b"scan" => self.cmd_scan(&args, out),
            b"RENAME" | b"rename" => self.cmd_rename(&args, out),
            b"RENAMENX" | b"renamenx" => self.cmd_renamenx(&args, out),
            b"RANDOMKEY" | b"randomkey" => self.cmd_randomkey(out),
            b"OBJECT" | b"object" => self.cmd_object(&args, out),
            b"COPY" | b"copy" => self.cmd_copy(&args, out),
            b"SUBSCRIBE" | b"subscribe" => self.cmd_subscribe(&args, out),
            b"UNSUBSCRIBE" | b"unsubscribe" => self.cmd_unsubscribe(&args, out),
            b"PUBLISH" | b"publish" => self.cmd_publish(&args, out),
            b"PSUBSCRIBE" | b"psubscribe" => self.cmd_psubscribe(&args, out),
            b"PUNSUBSCRIBE" | b"punsubscribe" => self.cmd_punsubscribe(&args, out),
            b"MULTI" | b"multi" => self.cmd_multi(out),
            b"EXEC" | b"exec" => self.cmd_exec(out),
            b"DISCARD" | b"discard" => self.cmd_discard(out),
            b"WATCH" | b"watch" => self.cmd_watch(&args, out),
            b"UNWATCH" | b"unwatch" => self.cmd_unwatch(out),
            b"WAIT" | b"wait" => self.cmd_wait(&args, out),
            b"SELECT" | b"select" => self.cmd_select(&args, out),
            b"DUMP" | b"dump" | b"DEBUG" | b"debug" | b"RESTORE" | b"restore" => {
                RespValue::err("not supported").serialize_into(out);
            }
            _ => {
                let cmd_str = String::from_utf8_lossy(&cmd);
                RespValue::err(&format!("Unknown command: {}", cmd_str)).serialize_into(out);
            }
        }
    }

    #[inline]
    fn cmd_ping(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() > 1 {
            if let RespValue::BulkString(Some(data)) = &args[1] {
                out.push(b'$');
                out.extend_from_slice(itoa::Buffer::new().format(data.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(data);
                out.extend_from_slice(b"\r\n");
                return;
            }
        }
        out.extend_from_slice(b"+PONG\r\n");
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

        // Zero-copy path: write RESP directly from DashMap guard
        if !self.current_handler().get_value_into(key, out) {
            out.extend_from_slice(b"$-1\r\n");
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

        // Fast path: simple SET key value (no options) — most common case
        if args.len() == 3 {
            match self.current_handler().put_sync(key, value, 0) {
                Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
            }
            return;
        }

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
            if self.current_handler().get_value(key).is_some() {
                out.extend_from_slice(b"$-1\r\n");
                return;
            }
        }

        // XX: only set if key EXISTS
        if opts.xx {
            if self.current_handler().get_value(key).is_none() {
                out.extend_from_slice(b"$-1\r\n");
                return;
            }
        }

        // GET: capture old value before writing
        let old_value = if opts.get {
            self.current_handler().get_value(key)
        } else {
            None
        };

        // Determine final TTL
        let final_ttl = if opts.keepttl {
            // Preserve existing TTL
            self.current_handler()
                .get_with_meta(key)
                .map(|m| m.ttl_secs)
                .unwrap_or(0)
        } else {
            opts.ttl_secs
        };

        match self.current_handler().put_sync(key, value, final_ttl) {
            Ok(_) => {
                if opts.get {
                    match old_value {
                        Some(v) => RespValue::bulk(v).serialize_into(out),
                        None => out.extend_from_slice(b"$-1\r\n"),
                    }
                } else {
                    out.extend_from_slice(b"+OK\r\n");
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
                if let Ok(true) = self.current_handler().delete_sync(key) {
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
                match self.current_handler().get_value(key) {
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

            if let Err(e) = self.current_handler().put_sync(key, value, 0) {
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
                if self.current_handler().get_value(key).is_some() {
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
        RespValue::Integer(self.current_handler().live_entries() as i64).serialize_into(out);
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

    // ── Helper: parse an integer argument ──────────────────────────────
    fn parse_int_arg(arg: &RespValue) -> Result<i64, String> {
        match arg {
            RespValue::BulkString(Some(v)) => {
                std::str::from_utf8(v)
                    .map_err(|_| "value is not an integer or out of range".to_string())
                    .and_then(|s| s.parse::<i64>().map_err(|_| "value is not an integer or out of range".to_string()))
            }
            _ => Err("value is not an integer or out of range".to_string()),
        }
    }

    fn parse_uint_arg(arg: &RespValue) -> Result<u64, String> {
        match arg {
            RespValue::BulkString(Some(v)) => {
                std::str::from_utf8(v)
                    .map_err(|_| "value is not an integer or out of range".to_string())
                    .and_then(|s| s.parse::<u64>().map_err(|_| "value is not an integer or out of range".to_string()))
            }
            _ => Err("value is not an integer or out of range".to_string()),
        }
    }

    fn extract_bulk_arg<'a>(arg: &'a RespValue) -> Option<&'a [u8]> {
        match arg {
            RespValue::BulkString(Some(data)) => Some(data.as_slice()),
            _ => None,
        }
    }

    // ── ECHO ──────────────────────────────────────────────────────────
    fn cmd_echo(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'echo' command").serialize_into(out);
            return;
        }
        if let RespValue::BulkString(Some(data)) = &args[1] {
            RespValue::bulk(data.clone()).serialize_into(out);
        } else {
            RespValue::err("Invalid argument").serialize_into(out);
        }
    }

    // ── TIME ──────────────────────────────────────────────────────────
    fn cmd_time(&self, out: &mut Vec<u8>) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        let micros = now.subsec_micros();
        RespValue::Array(Some(vec![
            RespValue::BulkString(Some(secs.to_string().into_bytes())),
            RespValue::BulkString(Some(micros.to_string().into_bytes())),
        ])).serialize_into(out);
    }

    // ── TYPE ──────────────────────────────────────────────────────────
    fn cmd_type(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'type' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => {
                RespValue::err("Invalid key").serialize_into(out);
                return;
            }
        };
        if self.check_moved_read(key, out) {
            return;
        }
        if self.current_handler().get_value(key).is_some() {
            RespValue::SimpleString("string".to_string()).serialize_into(out);
        } else {
            RespValue::SimpleString("none".to_string()).serialize_into(out);
        }
    }

    // ── FLUSHDB / FLUSHALL ────────────────────────────────────────────
    fn cmd_flushdb(&self, out: &mut Vec<u8>) {
        self.current_handler().clear();
        RespValue::ok().serialize_into(out);
    }

    // ── SELECT ────────────────────────────────────────────────────────
    fn cmd_select(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'select' command").serialize_into(out);
            return;
        }
        match Self::parse_int_arg(&args[1]) {
            Ok(db_num) if db_num >= 0 && (db_num as u8) < self.db_manager.num_dbs() => {
                self.current_db.set(db_num as u8);
                RespValue::ok().serialize_into(out);
            }
            Ok(_) => RespValue::err("ERR DB index is out of range").serialize_into(out),
            Err(e) => RespValue::err(&e).serialize_into(out),
        }
    }

    // ── WAIT ──────────────────────────────────────────────────────────
    fn cmd_wait(&self, args: &[RespValue], out: &mut Vec<u8>) {
        // WAIT numreplicas timeout - we always return 0 replicas in standalone
        RespValue::Integer(0).serialize_into(out);
    }

    // ── INCR ──────────────────────────────────────────────────────────
    fn cmd_incr(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'incr' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        self.incr_by_int(key, 1, out);
    }

    // ── DECR ──────────────────────────────────────────────────────────
    fn cmd_decr(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'decr' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        self.incr_by_int(key, -1, out);
    }

    // ── INCRBY ────────────────────────────────────────────────────────
    fn cmd_incrby(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("ERR wrong number of arguments for 'incrby' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        let increment = match Self::parse_int_arg(&args[2]) {
            Ok(v) => v,
            Err(e) => { RespValue::err(&e).serialize_into(out); return; }
        };
        self.incr_by_int(key, increment, out);
    }

    // ── DECRBY ────────────────────────────────────────────────────────
    fn cmd_decrby(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("ERR wrong number of arguments for 'decrby' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        let decrement = match Self::parse_int_arg(&args[2]) {
            Ok(v) => v,
            Err(e) => { RespValue::err(&e).serialize_into(out); return; }
        };
        self.incr_by_int(key, -decrement, out);
    }

    /// Shared implementation for INCR/DECR/INCRBY/DECRBY
    fn incr_by_int(&self, key: &[u8], delta: i64, out: &mut Vec<u8>) {
        let current = match self.current_handler().get_value(key) {
            Some(v) => {
                match std::str::from_utf8(&v).ok().and_then(|s| s.parse::<i64>().ok()) {
                    Some(n) => n,
                    None => {
                        RespValue::err("ERR value is not an integer or out of range").serialize_into(out);
                        return;
                    }
                }
            }
            None => 0,
        };
        let new_val = match current.checked_add(delta) {
            Some(v) => v,
            None => {
                RespValue::err("ERR increment or decrement would overflow").serialize_into(out);
                return;
            }
        };
        let new_bytes = new_val.to_string().into_bytes();
        // Preserve existing TTL
        let ttl = self.current_handler().get_with_meta(key).map(|m| m.ttl_secs).unwrap_or(0);
        match self.current_handler().put_sync(key, &new_bytes, ttl) {
            Ok(_) => RespValue::Integer(new_val).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    // ── INCRBYFLOAT ───────────────────────────────────────────────────
    fn cmd_incrbyfloat(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("ERR wrong number of arguments for 'incrbyfloat' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        let increment = match Self::extract_bulk_arg(&args[2]) {
            Some(v) => {
                match std::str::from_utf8(v).ok().and_then(|s| s.parse::<f64>().ok()) {
                    Some(f) if f.is_finite() => f,
                    _ => {
                        RespValue::err("ERR value is not a valid float").serialize_into(out);
                        return;
                    }
                }
            }
            None => { RespValue::err("ERR value is not a valid float").serialize_into(out); return; }
        };
        let current = match self.current_handler().get_value(key) {
            Some(v) => {
                match std::str::from_utf8(&v).ok().and_then(|s| s.parse::<f64>().ok()) {
                    Some(f) => f,
                    None => {
                        RespValue::err("ERR value is not a valid float").serialize_into(out);
                        return;
                    }
                }
            }
            None => 0.0,
        };
        let new_val = current + increment;
        if !new_val.is_finite() {
            RespValue::err("ERR increment would produce NaN or Infinity").serialize_into(out);
            return;
        }
        let new_str = format_float(new_val);
        let new_bytes = new_str.as_bytes().to_vec();
        let ttl = self.current_handler().get_with_meta(key).map(|m| m.ttl_secs).unwrap_or(0);
        match self.current_handler().put_sync(key, &new_bytes, ttl) {
            Ok(_) => RespValue::bulk(new_bytes).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    // ── APPEND ────────────────────────────────────────────────────────
    fn cmd_append(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("ERR wrong number of arguments for 'append' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        let suffix = match Self::extract_bulk_arg(&args[2]) {
            Some(v) => v,
            None => { RespValue::err("Invalid value").serialize_into(out); return; }
        };
        let mut new_val = self.current_handler().get_value(key).unwrap_or_default();
        new_val.extend_from_slice(suffix);
        let new_len = new_val.len() as i64;
        let ttl = self.current_handler().get_with_meta(key).map(|m| m.ttl_secs).unwrap_or(0);
        match self.current_handler().put_sync(key, &new_val, ttl) {
            Ok(_) => RespValue::Integer(new_len).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    // ── STRLEN ────────────────────────────────────────────────────────
    fn cmd_strlen(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'strlen' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved_read(key, out) { return; }
        let len = self.current_handler().get_value(key).map(|v| v.len() as i64).unwrap_or(0);
        RespValue::Integer(len).serialize_into(out);
    }

    // ── GETRANGE ──────────────────────────────────────────────────────
    fn cmd_getrange(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 4 {
            RespValue::err("ERR wrong number of arguments for 'getrange' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved_read(key, out) { return; }
        let start = match Self::parse_int_arg(&args[2]) {
            Ok(v) => v,
            Err(e) => { RespValue::err(&e).serialize_into(out); return; }
        };
        let end = match Self::parse_int_arg(&args[3]) {
            Ok(v) => v,
            Err(e) => { RespValue::err(&e).serialize_into(out); return; }
        };
        let value = self.current_handler().get_value(key).unwrap_or_default();
        let len = value.len() as i64;
        if len == 0 {
            RespValue::bulk(Vec::new()).serialize_into(out);
            return;
        }
        // Normalize negative indices
        let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
        let e = if end < 0 { (len + end).max(0) } else { end.min(len - 1) } as usize;
        if s > e || s >= value.len() {
            RespValue::bulk(Vec::new()).serialize_into(out);
        } else {
            RespValue::bulk(value[s..=e].to_vec()).serialize_into(out);
        }
    }

    // ── SETRANGE ──────────────────────────────────────────────────────
    fn cmd_setrange(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 4 {
            RespValue::err("ERR wrong number of arguments for 'setrange' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        let offset = match Self::parse_int_arg(&args[2]) {
            Ok(v) if v >= 0 => v as usize,
            _ => {
                RespValue::err("ERR offset is out of range").serialize_into(out);
                return;
            }
        };
        let replacement = match Self::extract_bulk_arg(&args[3]) {
            Some(v) => v,
            None => { RespValue::err("Invalid value").serialize_into(out); return; }
        };
        let mut value = self.current_handler().get_value(key).unwrap_or_default();
        let needed = offset + replacement.len();
        if needed > value.len() {
            value.resize(needed, 0);
        }
        value[offset..offset + replacement.len()].copy_from_slice(replacement);
        let new_len = value.len() as i64;
        let ttl = self.current_handler().get_with_meta(key).map(|m| m.ttl_secs).unwrap_or(0);
        match self.current_handler().put_sync(key, &value, ttl) {
            Ok(_) => RespValue::Integer(new_len).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    // ── SETNX ─────────────────────────────────────────────────────────
    fn cmd_setnx(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("ERR wrong number of arguments for 'setnx' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        if self.current_handler().get_value(key).is_some() {
            RespValue::Integer(0).serialize_into(out);
            return;
        }
        let value = match Self::extract_bulk_arg(&args[2]) {
            Some(v) => v,
            None => { RespValue::err("Invalid value").serialize_into(out); return; }
        };
        match self.current_handler().put_sync(key, value, 0) {
            Ok(_) => RespValue::Integer(1).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    // ── SETEX ─────────────────────────────────────────────────────────
    fn cmd_setex(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 4 {
            RespValue::err("ERR wrong number of arguments for 'setex' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        let secs = match Self::parse_int_arg(&args[2]) {
            Ok(v) if v > 0 => v as u32,
            _ => {
                RespValue::err("ERR invalid expire time in 'setex' command").serialize_into(out);
                return;
            }
        };
        let value = match Self::extract_bulk_arg(&args[3]) {
            Some(v) => v,
            None => { RespValue::err("Invalid value").serialize_into(out); return; }
        };
        match self.current_handler().put_sync(key, value, secs) {
            Ok(_) => RespValue::ok().serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    // ── PSETEX ────────────────────────────────────────────────────────
    fn cmd_psetex(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 4 {
            RespValue::err("ERR wrong number of arguments for 'psetex' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        let ms = match Self::parse_int_arg(&args[2]) {
            Ok(v) if v > 0 => v as u64,
            _ => {
                RespValue::err("ERR invalid expire time in 'psetex' command").serialize_into(out);
                return;
            }
        };
        let value = match Self::extract_bulk_arg(&args[3]) {
            Some(v) => v,
            None => { RespValue::err("Invalid value").serialize_into(out); return; }
        };
        let secs = (ms / 1000).max(1) as u32;
        match self.current_handler().put_sync(key, value, secs) {
            Ok(_) => RespValue::ok().serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    // ── GETDEL ────────────────────────────────────────────────────────
    fn cmd_getdel(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'getdel' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        match self.current_handler().get_value(key) {
            Some(value) => {
                let _ = self.current_handler().delete_sync(key);
                RespValue::bulk(value).serialize_into(out);
            }
            None => RespValue::null().serialize_into(out),
        }
    }

    // ── GETEX ─────────────────────────────────────────────────────────
    fn cmd_getex(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'getex' command").serialize_into(out);
            return;
        }
        let key = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(key, out) { return; }
        let value = match self.current_handler().get_value(key) {
            Some(v) => v,
            None => { RespValue::null().serialize_into(out); return; }
        };

        // Parse GETEX options: EX, PX, EXAT, PXAT, PERSIST
        if args.len() > 2 {
            if let Some(opt) = Self::extract_bulk_arg(&args[2]) {
                match opt {
                    b"EX" | b"ex" => {
                        if args.len() < 4 {
                            RespValue::err("syntax error").serialize_into(out); return;
                        }
                        match Self::parse_int_arg(&args[3]) {
                            Ok(s) if s > 0 => { let _ = self.current_handler().update_ttl(key, s as u32); }
                            _ => { RespValue::err("ERR invalid expire time").serialize_into(out); return; }
                        }
                    }
                    b"PX" | b"px" => {
                        if args.len() < 4 {
                            RespValue::err("syntax error").serialize_into(out); return;
                        }
                        match Self::parse_int_arg(&args[3]) {
                            Ok(ms) if ms > 0 => { let _ = self.current_handler().update_ttl(key, (ms as u64 / 1000).max(1) as u32); }
                            _ => { RespValue::err("ERR invalid expire time").serialize_into(out); return; }
                        }
                    }
                    b"EXAT" | b"exat" => {
                        if args.len() < 4 {
                            RespValue::err("syntax error").serialize_into(out); return;
                        }
                        match Self::parse_uint_arg(&args[3]) {
                            Ok(ts) => {
                                let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
                                if ts > now {
                                    let _ = self.current_handler().update_ttl(key, (ts - now) as u32);
                                }
                            }
                            Err(e) => { RespValue::err(&e).serialize_into(out); return; }
                        }
                    }
                    b"PXAT" | b"pxat" => {
                        if args.len() < 4 {
                            RespValue::err("syntax error").serialize_into(out); return;
                        }
                        match Self::parse_uint_arg(&args[3]) {
                            Ok(ts_ms) => {
                                let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                                if ts_ms > now_ms {
                                    let _ = self.current_handler().update_ttl(key, ((ts_ms - now_ms) / 1000).max(1) as u32);
                                }
                            }
                            Err(e) => { RespValue::err(&e).serialize_into(out); return; }
                        }
                    }
                    b"PERSIST" | b"persist" => {
                        let _ = self.current_handler().update_ttl(key, 0);
                    }
                    _ => {
                        RespValue::err("ERR unsupported option").serialize_into(out); return;
                    }
                }
            }
        }
        RespValue::bulk(value).serialize_into(out);
    }

    // ── KEYS ──────────────────────────────────────────────────────────
    fn cmd_keys(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'keys' command").serialize_into(out);
            return;
        }
        let pattern = match Self::extract_bulk_arg(&args[1]) {
            Some(p) => p,
            None => { RespValue::err("Invalid pattern").serialize_into(out); return; }
        };

        let mut matching_keys: Vec<Vec<u8>> = Vec::new();
        self.current_handler().iter_keys(|key_bytes| {
            if glob_match(pattern, key_bytes) {
                matching_keys.push(key_bytes.to_vec());
            }
        });

        let items: Vec<RespValue> = matching_keys.into_iter()
            .map(|k| RespValue::BulkString(Some(k)))
            .collect();
        RespValue::Array(Some(items)).serialize_into(out);
    }

    // ── SCAN ──────────────────────────────────────────────────────────
    fn cmd_scan(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'scan' command").serialize_into(out);
            return;
        }
        let cursor = match Self::parse_uint_arg(&args[1]) {
            Ok(c) => c,
            Err(e) => { RespValue::err(&e).serialize_into(out); return; }
        };

        // Parse optional MATCH and COUNT
        let mut pattern: Option<&[u8]> = None;
        let mut count: usize = 10;
        let mut i = 2;
        while i < args.len() {
            if let Some(opt) = Self::extract_bulk_arg(&args[i]) {
                match opt {
                    b"MATCH" | b"match" => {
                        if i + 1 < args.len() {
                            pattern = Self::extract_bulk_arg(&args[i + 1]);
                            i += 2;
                            continue;
                        }
                    }
                    b"COUNT" | b"count" => {
                        if i + 1 < args.len() {
                            if let Ok(c) = Self::parse_uint_arg(&args[i + 1]) {
                                count = c as usize;
                            }
                            i += 2;
                            continue;
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }

        // Cursor encoding: high 8 bits = shard index, low 56 bits = position within shard
        let start_shard = ((cursor >> 56) & 0xFF) as usize;
        let start_pos = (cursor & 0x00FFFFFFFFFFFFFF) as usize;

        let mut results: Vec<Vec<u8>> = Vec::new();
        let (next_shard, next_pos, done) = self.current_handler().scan_keys(
            start_shard,
            start_pos,
            count,
            pattern,
            &mut results,
            glob_match,
        );

        let next_cursor = if done {
            0u64
        } else {
            ((next_shard as u64) << 56) | (next_pos as u64 & 0x00FFFFFFFFFFFFFF)
        };

        let items: Vec<RespValue> = results.into_iter()
            .map(|k| RespValue::BulkString(Some(k)))
            .collect();
        RespValue::Array(Some(vec![
            RespValue::BulkString(Some(next_cursor.to_string().into_bytes())),
            RespValue::Array(Some(items)),
        ])).serialize_into(out);
    }

    // ── RENAME ────────────────────────────────────────────────────────
    fn cmd_rename(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("ERR wrong number of arguments for 'rename' command").serialize_into(out);
            return;
        }
        let src = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        let dst = match Self::extract_bulk_arg(&args[2]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(src, out) { return; }

        let meta = match self.current_handler().get_with_meta(src) {
            Some(m) => m,
            None => {
                RespValue::err("ERR no such key").serialize_into(out);
                return;
            }
        };
        // Delete old key, set new key preserving TTL
        let _ = self.current_handler().delete_sync(src);
        match self.current_handler().put_sync(dst, &meta.value, meta.ttl_secs) {
            Ok(_) => RespValue::ok().serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    // ── RENAMENX ──────────────────────────────────────────────────────
    fn cmd_renamenx(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("ERR wrong number of arguments for 'renamenx' command").serialize_into(out);
            return;
        }
        let src = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        let dst = match Self::extract_bulk_arg(&args[2]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(src, out) { return; }

        let meta = match self.current_handler().get_with_meta(src) {
            Some(m) => m,
            None => {
                RespValue::err("ERR no such key").serialize_into(out);
                return;
            }
        };
        if self.current_handler().get_value(dst).is_some() {
            RespValue::Integer(0).serialize_into(out);
            return;
        }
        let _ = self.current_handler().delete_sync(src);
        match self.current_handler().put_sync(dst, &meta.value, meta.ttl_secs) {
            Ok(_) => RespValue::Integer(1).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    // ── RANDOMKEY ─────────────────────────────────────────────────────
    fn cmd_randomkey(&self, out: &mut Vec<u8>) {
        match self.current_handler().random_key() {
            Some(key) => RespValue::bulk(key).serialize_into(out),
            None => RespValue::null().serialize_into(out),
        }
    }

    // ── OBJECT ────────────────────────────────────────────────────────
    fn cmd_object(&self, args: &[RespValue], out: &mut Vec<u8>) {
        let subcmd = if args.len() > 1 {
            Self::extract_bulk_arg(&args[1]).unwrap_or(b"")
        } else {
            b""
        };
        match subcmd {
            b"HELP" | b"help" => {
                let help = vec![
                    RespValue::BulkString(Some(b"OBJECT ENCODING <key> - Return the encoding of the object stored at <key>".to_vec())),
                    RespValue::BulkString(Some(b"OBJECT HELP - Return this help message".to_vec())),
                ];
                RespValue::Array(Some(help)).serialize_into(out);
            }
            b"ENCODING" | b"encoding" => {
                if args.len() < 3 {
                    RespValue::err("ERR wrong number of arguments").serialize_into(out);
                    return;
                }
                let key = match Self::extract_bulk_arg(&args[2]) {
                    Some(k) => k,
                    None => { RespValue::err("Invalid key").serialize_into(out); return; }
                };
                if self.current_handler().get_value(key).is_some() {
                    RespValue::bulk(b"raw".to_vec()).serialize_into(out);
                } else {
                    RespValue::err("ERR no such key").serialize_into(out);
                }
            }
            _ => {
                RespValue::err("ERR Unknown OBJECT subcommand").serialize_into(out);
            }
        }
    }

    // ── COPY ──────────────────────────────────────────────────────────
    fn cmd_copy(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("ERR wrong number of arguments for 'copy' command").serialize_into(out);
            return;
        }
        let src = match Self::extract_bulk_arg(&args[1]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        let dst = match Self::extract_bulk_arg(&args[2]) {
            Some(k) => k,
            None => { RespValue::err("Invalid key").serialize_into(out); return; }
        };
        if self.check_moved(src, out) { return; }

        // Check for REPLACE option
        let replace = args[3..].iter().any(|a| {
            Self::extract_bulk_arg(a).map(|v| v.eq_ignore_ascii_case(b"REPLACE")).unwrap_or(false)
        });

        let meta = match self.current_handler().get_with_meta(src) {
            Some(m) => m,
            None => {
                RespValue::Integer(0).serialize_into(out);
                return;
            }
        };
        if !replace && self.current_handler().get_value(dst).is_some() {
            RespValue::Integer(0).serialize_into(out);
            return;
        }
        match self.current_handler().put_sync(dst, &meta.value, meta.ttl_secs) {
            Ok(_) => RespValue::Integer(1).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    // ── SUBSCRIBE ─────────────────────────────────────────────────────
    fn cmd_subscribe(&self, args: &[RespValue], out: &mut Vec<u8>) {
        // In a real implementation, this would put the connection into pub/sub mode.
        // For now, we acknowledge the subscription.
        let mut sub_count = 0i64;
        for arg in &args[1..] {
            if let Some(channel) = Self::extract_bulk_arg(arg) {
                sub_count += 1;
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"subscribe".to_vec())),
                    RespValue::BulkString(Some(channel.to_vec())),
                    RespValue::Integer(sub_count),
                ])).serialize_into(out);
            }
        }
    }

    // ── UNSUBSCRIBE ───────────────────────────────────────────────────
    fn cmd_unsubscribe(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            // Unsubscribe from all
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"unsubscribe".to_vec())),
                RespValue::null(),
                RespValue::Integer(0),
            ])).serialize_into(out);
            return;
        }
        let mut remaining = 0i64;
        for arg in &args[1..] {
            if let Some(channel) = Self::extract_bulk_arg(arg) {
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"unsubscribe".to_vec())),
                    RespValue::BulkString(Some(channel.to_vec())),
                    RespValue::Integer(remaining),
                ])).serialize_into(out);
            }
        }
    }

    // ── PUBLISH ───────────────────────────────────────────────────────
    fn cmd_publish(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("ERR wrong number of arguments for 'publish' command").serialize_into(out);
            return;
        }
        let channel = match Self::extract_bulk_arg(&args[1]) {
            Some(c) => c,
            None => { RespValue::err("Invalid channel").serialize_into(out); return; }
        };
        let message = match Self::extract_bulk_arg(&args[2]) {
            Some(m) => m,
            None => { RespValue::err("Invalid message").serialize_into(out); return; }
        };
        let count = self.pubsub.publish(channel, message);
        RespValue::Integer(count).serialize_into(out);
    }

    // ── PSUBSCRIBE ────────────────────────────────────────────────────
    fn cmd_psubscribe(&self, args: &[RespValue], out: &mut Vec<u8>) {
        let mut sub_count = 0i64;
        for arg in &args[1..] {
            if let Some(pattern) = Self::extract_bulk_arg(arg) {
                sub_count += 1;
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"psubscribe".to_vec())),
                    RespValue::BulkString(Some(pattern.to_vec())),
                    RespValue::Integer(sub_count),
                ])).serialize_into(out);
            }
        }
    }

    // ── PUNSUBSCRIBE ──────────────────────────────────────────────────
    fn cmd_punsubscribe(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"punsubscribe".to_vec())),
                RespValue::null(),
                RespValue::Integer(0),
            ])).serialize_into(out);
            return;
        }
        for arg in &args[1..] {
            if let Some(pattern) = Self::extract_bulk_arg(arg) {
                RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"punsubscribe".to_vec())),
                    RespValue::BulkString(Some(pattern.to_vec())),
                    RespValue::Integer(0),
                ])).serialize_into(out);
            }
        }
    }

    // ── MULTI ─────────────────────────────────────────────────────────
    fn cmd_multi(&self, out: &mut Vec<u8>) {
        let mut tx = self.tx_queue.borrow_mut();
        if tx.is_some() {
            RespValue::err("ERR MULTI calls can not be nested").serialize_into(out);
            return;
        }
        *tx = Some(Vec::new());
        RespValue::ok().serialize_into(out);
    }

    // ── EXEC ──────────────────────────────────────────────────────────
    fn cmd_exec(&self, out: &mut Vec<u8>) {
        let queued = {
            let mut tx = self.tx_queue.borrow_mut();
            match tx.take() {
                Some(q) => q,
                None => {
                    RespValue::err("ERR EXEC without MULTI").serialize_into(out);
                    return;
                }
            }
        };

        // Check watched keys
        {
            let watched = self.watched_keys.borrow();
            for (key, gen_at_watch) in watched.iter() {
                let current_gen = self.current_handler().get_generation(key);
                if current_gen != *gen_at_watch {
                    // Watched key changed - abort transaction
                    self.watched_keys.borrow_mut().clear();
                    RespValue::Array(None).serialize_into(out);
                    return;
                }
            }
        }
        self.watched_keys.borrow_mut().clear();

        // Execute queued commands
        let count = queued.len();
        out.push(b'*');
        out.extend_from_slice(itoa::Buffer::new().format(count).as_bytes());
        out.extend_from_slice(b"\r\n");

        for args in queued {
            self.handle_command(RespValue::Array(Some(args)), out);
        }
    }

    // ── DISCARD ───────────────────────────────────────────────────────
    fn cmd_discard(&self, out: &mut Vec<u8>) {
        let mut tx = self.tx_queue.borrow_mut();
        if tx.is_none() {
            RespValue::err("ERR DISCARD without MULTI").serialize_into(out);
            return;
        }
        *tx = None;
        self.watched_keys.borrow_mut().clear();
        RespValue::ok().serialize_into(out);
    }

    // ── WATCH ─────────────────────────────────────────────────────────
    fn cmd_watch(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("ERR wrong number of arguments for 'watch' command").serialize_into(out);
            return;
        }
        if self.tx_queue.borrow().is_some() {
            RespValue::err("ERR WATCH inside MULTI is not allowed").serialize_into(out);
            return;
        }
        let mut watched = self.watched_keys.borrow_mut();
        for arg in &args[1..] {
            if let Some(key) = Self::extract_bulk_arg(arg) {
                let gen = self.current_handler().get_generation(key);
                watched.insert(key.to_vec(), gen);
            }
        }
        RespValue::ok().serialize_into(out);
    }

    // ── UNWATCH ───────────────────────────────────────────────────────
    fn cmd_unwatch(&self, out: &mut Vec<u8>) {
        self.watched_keys.borrow_mut().clear();
        RespValue::ok().serialize_into(out);
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

        match self.current_handler().get_with_meta(key) {
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

        match self.current_handler().get_with_meta(key) {
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

        match self.current_handler().update_ttl(key, secs) {
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
        match self.current_handler().update_ttl(key, secs) {
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
        match self.current_handler().get_with_meta(key) {
            None => RespValue::Integer(0).serialize_into(out),
            Some(meta) => {
                if meta.ttl_secs == 0 {
                    // No TTL to remove
                    RespValue::Integer(0).serialize_into(out);
                } else {
                    match self.current_handler().update_ttl(key, 0) {
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
            let _ = self.current_handler().delete_sync(key);
            RespValue::Integer(1).serialize_into(out);
            return;
        }

        let remaining = (timestamp - now_secs) as u32;
        match self.current_handler().update_ttl(key, remaining) {
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
            let _ = self.current_handler().delete_sync(key);
            RespValue::Integer(1).serialize_into(out);
            return;
        }

        let remaining_secs = ((timestamp_ms - now_ms) / 1000) as u32;
        match self.current_handler().update_ttl(key, remaining_secs) {
            Ok(true) => RespValue::Integer(1).serialize_into(out),
            Ok(false) => RespValue::Integer(0).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }
}

/// Glob-style pattern matching (supports *, ?, [abc], [a-z], [^a]).
fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut pi = 0;
    let mut si = 0;
    let mut star_pi = usize::MAX;
    let mut star_si = 0;

    while si < string.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == string[si]) {
            pi += 1;
            si += 1;
        } else if pi < pattern.len() && pattern[pi] == b'[' {
            // Character class
            pi += 1;
            let negate = pi < pattern.len() && (pattern[pi] == b'^' || pattern[pi] == b'!');
            if negate {
                pi += 1;
            }
            let mut matched = false;
            let mut first = true;
            while pi < pattern.len() && (first || pattern[pi] != b']') {
                first = false;
                if pi + 2 < pattern.len() && pattern[pi + 1] == b'-' {
                    if string[si] >= pattern[pi] && string[si] <= pattern[pi + 2] {
                        matched = true;
                    }
                    pi += 3;
                } else {
                    if string[si] == pattern[pi] {
                        matched = true;
                    }
                    pi += 1;
                }
            }
            if pi < pattern.len() {
                pi += 1; // skip ]
            }
            if matched == negate {
                // Match failed for this character class
                if star_pi != usize::MAX {
                    pi = star_pi + 1;
                    star_si += 1;
                    si = star_si;
                } else {
                    return false;
                }
            } else {
                si += 1;
            }
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_si = si;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }

    // Skip trailing *
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// Format a float like Redis does (use enough precision, trim trailing zeros).
fn format_float(f: f64) -> String {
    // Redis uses up to 17 significant digits, then trims trailing zeros
    let s = format!("{}", f);
    s
}

/// High-performance Redis server with pipelining
pub struct RedisServer {
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    router: Option<Arc<ClusterRouter>>,
    replica_read: bool,
    pubsub: Arc<PubSubManager>,
    tuning: ServerTuning,
    live_conns: Arc<std::sync::atomic::AtomicUsize>,
}

/// RAII guard: increments on construction (after the cap check), decrements on drop.
struct ConnectionGuard {
    live: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.live.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl RedisServer {
    pub fn new(addr: SocketAddr, db_manager: Arc<DatabaseManager>, tuning: ServerTuning) -> Self {
        Self {
            addr,
            db_manager,
            router: None,
            replica_read: false,
            pubsub: Arc::new(PubSubManager::new()),
            tuning,
            live_conns: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub fn with_router(addr: SocketAddr, db_manager: Arc<DatabaseManager>, router: Arc<ClusterRouter>, replica_read: bool, tuning: ServerTuning) -> Self {
        Self {
            addr,
            db_manager,
            router: Some(router),
            replica_read,
            pubsub: Arc::new(PubSubManager::new()),
            tuning,
            live_conns: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(self.addr)?;
        info!("Redis server listening on {}", self.addr);

        // Set listener to non-blocking for graceful shutdown
        listener.set_nonblocking(false)?;

        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    // Enforce --max-connections. Load then CAS so we never exceed the cap
                    // even under concurrent accepts (future reactor); Relaxed is fine —
                    // the exact count at the boundary is allowed to be approximate.
                    use std::sync::atomic::Ordering;
                    let max = self.tuning.max_connections;
                    let mut cur = self.live_conns.load(Ordering::Relaxed);
                    let accepted = loop {
                        if cur >= max {
                            break false;
                        }
                        match self.live_conns.compare_exchange_weak(
                            cur,
                            cur + 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break true,
                            Err(actual) => cur = actual,
                        }
                    };
                    if !accepted {
                        debug!("Rejecting connection; at --max-connections cap {}", max);
                        let _ = stream.write_all(b"-ERR max number of clients reached\r\n");
                        // Dropping the stream closes it.
                        continue;
                    }

                    let guard = ConnectionGuard { live: Arc::clone(&self.live_conns) };
                    let db_manager = Arc::clone(&self.db_manager);
                    let router = self.router.clone();
                    let replica_read = self.replica_read;
                    let pubsub = Arc::clone(&self.pubsub);
                    let tuning = self.tuning;
                    std::thread::spawn(move || {
                        // guard is moved in and decrements on drop when the handler returns.
                        let _guard = guard;
                        if let Err(e) = handle_redis_client_fast(stream, db_manager, router, replica_read, pubsub, tuning) {
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
    db_manager: Arc<DatabaseManager>,
    router: Option<Arc<ClusterRouter>>,
    replica_read: bool,
    pubsub: Arc<PubSubManager>,
    tuning: ServerTuning,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(300)))?;

    let peer = stream.peer_addr()?;
    debug!("Redis client connected: {}", peer);

    let mut parser = RespParser::new(tuning.read_buf_bytes);
    let redis_handler = if let Some(router) = router.clone() {
        RedisHandler::with_router_and_pubsub(db_manager.clone(), router, replica_read, pubsub)
    } else {
        RedisHandler::new_with_pubsub(db_manager.clone(), pubsub)
    };
    let mut write_buf = Vec::with_capacity(tuning.write_buf_bytes);
    let mut commands_in_batch = 0;

    // Pre-resolve current handler for fast path (no cluster, db 0)
    let fast_handler = if router.is_none() {
        db_manager.db(0)
    } else {
        None
    };

    loop {
        // Try zero-allocation fast path for simple GET/SET (non-cluster, no transaction)
        if let Some(handler) = fast_handler {
            match parser.try_fast_command(handler, &mut write_buf) {
                Ok(Some(true)) => {
                    // Fast path handled the command
                    commands_in_batch += 1;
                    if write_buf.len() >= tuning.write_buf_bytes / 2 || commands_in_batch >= MAX_PIPELINE_DEPTH {
                        stream.write_all(&write_buf)?;
                        write_buf.clear();
                        commands_in_batch = 0;
                    }
                    // Check for end of pipeline
                    if parser.len == parser.pos && !write_buf.is_empty() {
                        stream.write_all(&write_buf)?;
                        write_buf.clear();
                        commands_in_batch = 0;
                    }
                    continue;
                }
                Ok(Some(false)) => {
                    // Not a fast command, fall through to full parse
                }
                Ok(None) => {
                    // Need more data — fill buffer and retry
                    if !parser.fill_buffer(&mut stream)? {
                        if !write_buf.is_empty() {
                            stream.write_all(&write_buf)?;
                        }
                        break;
                    }
                    continue;
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
                }
            }
        }

        match parser.try_parse(&mut stream) {
            Ok(Some(value)) => {
                // Handle command and buffer response
                redis_handler.handle_command(value, &mut write_buf);
                commands_in_batch += 1;

                // Flush if buffer is large or many commands processed
                if write_buf.len() >= tuning.write_buf_bytes / 2 || commands_in_batch >= MAX_PIPELINE_DEPTH {
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
pub fn start_redis_server(
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    tuning: ServerTuning,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let server = RedisServer::new(addr, db_manager, tuning);
        if let Err(e) = server.run() {
            error!("Redis server error: {}", e);
        }
    })
}

/// Starts the Redis server with cluster routing on a separate thread
pub fn start_redis_server_clustered(
    addr: SocketAddr,
    db_manager: Arc<DatabaseManager>,
    router: Arc<ClusterRouter>,
    replica_read: bool,
    tuning: ServerTuning,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let server = RedisServer::with_router(addr, db_manager, router, replica_read, tuning);
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

    fn make_db_manager(dir: &std::path::Path) -> Arc<DatabaseManager> {
        let handler = make_handler(dir);
        Arc::new(DatabaseManager::new(vec![DbHandler::Ssd(handler)]))
    }

    fn make_db_manager_from_handler(handler: Arc<Handler>) -> Arc<DatabaseManager> {
        Arc::new(DatabaseManager::new(vec![DbHandler::Ssd(handler)]))
    }

    fn make_cmd(parts: &[&[u8]]) -> RespValue {
        RespValue::Array(Some(
            parts.iter().map(|p| RespValue::BulkString(Some(p.to_vec()))).collect()
        ))
    }

    #[test]
    fn test_readonly_command_responds_ok() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();

        rh.handle_command(make_cmd(&[b"READONLY"]), &mut out);
        assert_eq!(out, b"+OK\r\n");
        assert!(rh.readonly.get());
    }

    #[test]
    fn test_readwrite_command_responds_ok() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
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
        let db_manager = make_db_manager_from_handler(Arc::clone(&handler));
        let rh = RedisHandler::with_router(db_manager, router, true);

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
        handler.put_sync(key.as_bytes(), b"replica_val", 0).unwrap();

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
        let db_manager = make_db_manager_from_handler(handler);
        let rh = RedisHandler::with_router(db_manager, router, true);
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
        let db_manager = make_db_manager_from_handler(handler);
        // replica_read = false (server disabled)
        let rh = RedisHandler::with_router(db_manager, router, false);
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

    // ── Tests for new commands ───────────────────────────────────────

    #[test]
    fn test_echo() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"ECHO", b"hello world"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("hello world"), "ECHO should return the argument: {}", resp);
    }

    #[test]
    fn test_time() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"TIME"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.starts_with("*2\r\n"), "TIME should return 2-element array: {}", resp);
    }

    #[test]
    fn test_type_existing_key() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"mykey", b"val"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"TYPE", b"mykey"]), &mut out);
        assert_eq!(&out, b"+string\r\n");
    }

    #[test]
    fn test_type_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"TYPE", b"nokey"]), &mut out);
        assert_eq!(&out, b"+none\r\n");
    }

    #[test]
    fn test_dbsize() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"k1", b"v1"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"SET", b"k2", b"v2"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"DBSIZE"]), &mut out);
        assert_eq!(&out, b":2\r\n");
    }

    #[test]
    fn test_flushdb() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"k1", b"v1"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"FLUSHDB"]), &mut out);
        assert_eq!(&out, b"+OK\r\n");
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"k1"]), &mut out);
        assert_eq!(&out, b"$-1\r\n");
    }

    #[test]
    fn test_incr_new_key() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"INCR", b"counter"]), &mut out);
        assert_eq!(&out, b":1\r\n");
    }

    #[test]
    fn test_incr_existing() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"counter", b"10"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"INCR", b"counter"]), &mut out);
        assert_eq!(&out, b":11\r\n");
    }

    #[test]
    fn test_decr() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"counter", b"10"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"DECR", b"counter"]), &mut out);
        assert_eq!(&out, b":9\r\n");
    }

    #[test]
    fn test_incrby() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"counter", b"10"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"INCRBY", b"counter", b"5"]), &mut out);
        assert_eq!(&out, b":15\r\n");
    }

    #[test]
    fn test_decrby() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"counter", b"10"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"DECRBY", b"counter", b"3"]), &mut out);
        assert_eq!(&out, b":7\r\n");
    }

    #[test]
    fn test_incr_non_integer() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"str", b"not_a_number"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"INCR", b"str"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.starts_with("-ERR"), "Expected error: {}", resp);
    }

    #[test]
    fn test_incrbyfloat() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"flt", b"10.5"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"INCRBYFLOAT", b"flt", b"0.1"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("10.6"), "Expected 10.6: {}", resp);
    }

    #[test]
    fn test_append() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"mykey", b"Hello"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"APPEND", b"mykey", b" World"]), &mut out);
        assert_eq!(&out, b":11\r\n"); // "Hello World" = 11 bytes
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"mykey"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("Hello World"), "Expected 'Hello World': {}", resp);
    }

    #[test]
    fn test_strlen() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"mykey", b"Hello"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"STRLEN", b"mykey"]), &mut out);
        assert_eq!(&out, b":5\r\n");
    }

    #[test]
    fn test_strlen_missing() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"STRLEN", b"nokey"]), &mut out);
        assert_eq!(&out, b":0\r\n");
    }

    #[test]
    fn test_getrange() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"mykey", b"Hello World"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"GETRANGE", b"mykey", b"0", b"4"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("Hello"), "Expected 'Hello': {}", resp);
    }

    #[test]
    fn test_getrange_negative() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"mykey", b"Hello World"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"GETRANGE", b"mykey", b"-5", b"-1"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("World"), "Expected 'World': {}", resp);
    }

    #[test]
    fn test_setrange() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"mykey", b"Hello World"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"SETRANGE", b"mykey", b"6", b"Redis"]), &mut out);
        assert_eq!(&out, b":11\r\n");
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"mykey"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("Hello Redis"), "Expected 'Hello Redis': {}", resp);
    }

    #[test]
    fn test_setnx_new() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SETNX", b"mykey", b"Hello"]), &mut out);
        assert_eq!(&out, b":1\r\n");
    }

    #[test]
    fn test_setnx_existing() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"mykey", b"Hello"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"SETNX", b"mykey", b"World"]), &mut out);
        assert_eq!(&out, b":0\r\n");
    }

    #[test]
    fn test_setex() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SETEX", b"mykey", b"10", b"Hello"]), &mut out);
        assert_eq!(&out, b"+OK\r\n");
        out.clear();
        rh.handle_command(make_cmd(&[b"TTL", b"mykey"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.starts_with(":"), "Expected TTL integer: {}", resp);
        // TTL should be close to 10
        let ttl: i64 = resp.trim_start_matches(':').trim_end_matches("\r\n").parse().unwrap();
        assert!(ttl > 0 && ttl <= 10, "Expected TTL 1-10, got: {}", ttl);
    }

    #[test]
    fn test_getdel() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"mykey", b"Hello"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"GETDEL", b"mykey"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("Hello"), "GETDEL should return value: {}", resp);
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"mykey"]), &mut out);
        assert_eq!(&out, b"$-1\r\n", "Key should be deleted after GETDEL");
    }

    #[test]
    fn test_getex_with_persist() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"mykey", b"Hello"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"GETEX", b"mykey", b"EX", b"100"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("Hello"), "GETEX should return value: {}", resp);
        out.clear();
        rh.handle_command(make_cmd(&[b"TTL", b"mykey"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        let ttl: i64 = resp.trim_start_matches(':').trim_end_matches("\r\n").parse().unwrap();
        assert!(ttl > 0, "Expected positive TTL: {}", ttl);
    }

    #[test]
    fn test_keys_all() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"key1", b"v1"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"SET", b"key2", b"v2"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"SET", b"other", b"v3"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"KEYS", b"key*"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("key1"), "Should match key1: {}", resp);
        assert!(resp.contains("key2"), "Should match key2: {}", resp);
        assert!(!resp.contains("other"), "Should not match 'other': {}", resp);
    }

    #[test]
    fn test_scan_basic() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        for i in 0..5 {
            rh.handle_command(make_cmd(&[b"SET", format!("k{}", i).as_bytes(), b"v"]), &mut out);
            out.clear();
        }
        rh.handle_command(make_cmd(&[b"SCAN", b"0", b"COUNT", b"100"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        // Should return cursor 0 (complete) and all keys
        assert!(resp.starts_with("*2\r\n"), "SCAN should return 2-element array: {}", resp);
    }

    #[test]
    fn test_rename() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"old", b"value"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"RENAME", b"old", b"new"]), &mut out);
        assert_eq!(&out, b"+OK\r\n");
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"new"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("value"), "New key should have the value: {}", resp);
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"old"]), &mut out);
        assert_eq!(&out, b"$-1\r\n", "Old key should be gone");
    }

    #[test]
    fn test_rename_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"RENAME", b"nokey", b"new"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("ERR"), "Should error for missing key: {}", resp);
    }

    #[test]
    fn test_copy() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"src", b"value"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"COPY", b"src", b"dst"]), &mut out);
        assert_eq!(&out, b":1\r\n");
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"dst"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("value"), "Copied key should have value: {}", resp);
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"src"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("value"), "Source should still exist: {}", resp);
    }

    #[test]
    fn test_select_db0() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SELECT", b"0"]), &mut out);
        assert_eq!(&out, b"+OK\r\n");
    }

    #[test]
    fn test_select_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SELECT", b"1"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("ERR"), "SELECT 1 should fail: {}", resp);
    }

    #[test]
    fn test_multi_exec() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();

        // MULTI
        rh.handle_command(make_cmd(&[b"MULTI"]), &mut out);
        assert_eq!(&out, b"+OK\r\n");

        // Queue SET
        out.clear();
        rh.handle_command(make_cmd(&[b"SET", b"txkey", b"txval"]), &mut out);
        assert_eq!(out, b"+QUEUED\r\n");

        // Queue GET
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"txkey"]), &mut out);
        assert_eq!(out, b"+QUEUED\r\n");

        // EXEC
        out.clear();
        rh.handle_command(make_cmd(&[b"EXEC"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        // Should have *2 array with OK and the value
        assert!(resp.starts_with("*2\r\n"), "EXEC should return results array: {}", resp);
        assert!(resp.contains("+OK\r\n"), "Should contain SET result: {}", resp);
        assert!(resp.contains("txval"), "Should contain GET result: {}", resp);
    }

    #[test]
    fn test_discard() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();

        rh.handle_command(make_cmd(&[b"MULTI"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"SET", b"txkey", b"txval"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"DISCARD"]), &mut out);
        assert_eq!(&out, b"+OK\r\n");

        // Key should not exist
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"txkey"]), &mut out);
        assert_eq!(&out, b"$-1\r\n");
    }

    #[test]
    fn test_watch_exec_no_change() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();

        rh.handle_command(make_cmd(&[b"SET", b"wk", b"v1"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"WATCH", b"wk"]), &mut out);
        assert_eq!(&out, b"+OK\r\n");
        out.clear();
        rh.handle_command(make_cmd(&[b"MULTI"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"SET", b"wk", b"v2"]), &mut out);
        out.clear();

        // No external change, EXEC should succeed
        rh.handle_command(make_cmd(&[b"EXEC"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.starts_with("*1\r\n"), "EXEC should succeed: {}", resp);
    }

    #[test]
    fn test_publish_returns_zero_no_subscribers() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"PUBLISH", b"chan", b"msg"]), &mut out);
        assert_eq!(&out, b":0\r\n");
    }

    #[test]
    fn test_unlink_is_del_alias() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"k", b"v"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"UNLINK", b"k"]), &mut out);
        assert_eq!(&out, b":1\r\n");
        out.clear();
        rh.handle_command(make_cmd(&[b"GET", b"k"]), &mut out);
        assert_eq!(&out, b"$-1\r\n");
    }

    #[test]
    fn test_object_encoding() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();
        rh.handle_command(make_cmd(&[b"SET", b"k", b"v"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"OBJECT", b"ENCODING", b"k"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("raw"), "Expected 'raw' encoding: {}", resp);
    }

    #[test]
    fn test_glob_match_function() {
        // Test the glob_match utility directly
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
        assert!(glob_match(b"key*", b"key123"));
        assert!(glob_match(b"*key", b"mykey"));
        assert!(glob_match(b"*key*", b"mykey123"));
        assert!(!glob_match(b"key*", b"nokey"));
        assert!(glob_match(b"k[0-9]y", b"k5y"));
        assert!(!glob_match(b"k[0-9]y", b"kay"));
    }

    #[test]
    fn test_randomkey() {
        let dir = tempfile::tempdir().unwrap();
        let db_manager = make_db_manager(dir.path());
        let rh = RedisHandler::new(db_manager);
        let mut out = Vec::new();

        // Empty db
        rh.handle_command(make_cmd(&[b"RANDOMKEY"]), &mut out);
        assert_eq!(&out, b"$-1\r\n");

        // With a key
        out.clear();
        rh.handle_command(make_cmd(&[b"SET", b"somekey", b"v"]), &mut out);
        out.clear();
        rh.handle_command(make_cmd(&[b"RANDOMKEY"]), &mut out);
        let resp = String::from_utf8_lossy(&out);
        assert!(resp.contains("somekey"), "Should return somekey: {}", resp);
    }
}
