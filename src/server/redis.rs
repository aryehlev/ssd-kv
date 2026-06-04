//! Redis protocol (RESP) parser and command handler.
//!
//! Supports: PING, GET, SET, DEL, EXISTS, MGET, MSET, KEYS, SCAN,
//!           DBSIZE, SELECT, FLUSHDB, FLUSHALL, INFO, COMMAND, QUIT.

use std::cell::Cell;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use tracing::{debug, error};

use crate::server::db_manager::{DatabaseManager, DbHandler};

/// Maximum read buffer size (64 MB).
const MAX_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// Per-server tuning.
#[derive(Clone, Copy, Debug)]
pub struct ServerTuning {
    pub read_buf_bytes: usize,
    pub write_buf_bytes: usize,
    pub max_connections: usize,
}

impl ServerTuning {
    pub const fn default_test() -> Self {
        Self {
            read_buf_bytes: 64 * 1024,
            write_buf_bytes: 64 * 1024,
            max_connections: 10_000,
        }
    }
}

// ─── RESP types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
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

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        self.serialize_into(&mut buf);
        buf
    }

    #[inline] pub fn ok() -> Self { RespValue::SimpleString("OK".to_string()) }
    #[inline] pub fn pong() -> Self { RespValue::SimpleString("PONG".to_string()) }
    #[inline] pub fn null() -> Self { RespValue::BulkString(None) }
    #[inline] pub fn err(msg: &str) -> Self { RespValue::Error(format!("ERR {}", msg)) }
    #[inline] pub fn bulk(data: Vec<u8>) -> Self { RespValue::BulkString(Some(data)) }
}

// ─── RESP parser ─────────────────────────────────────────────────────────────

pub struct RespParser {
    buf: Vec<u8>,
    pos: usize,
    len: usize,
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

    /// Push bytes received from the network into the buffer.
    pub fn append_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        if self.pos > 0 {
            if self.pos < self.len {
                self.buf.copy_within(self.pos..self.len, 0);
                self.len -= self.pos;
            } else {
                self.len = 0;
            }
            self.pos = 0;
        }
        while self.len + data.len() > self.buf.len() {
            let new_size = (self.buf.len() * 2).min(MAX_BUFFER_SIZE);
            if new_size <= self.buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "RESP message exceeds maximum buffer size",
                ));
            }
            self.buf.resize(new_size, 0);
        }
        self.buf[self.len..self.len + data.len()].copy_from_slice(data);
        self.len += data.len();
        Ok(())
    }

    /// Pull the next complete RESP value from the buffer, if any.
    pub fn next_value(&mut self) -> io::Result<Option<RespValue>> {
        self.parse_value()
    }

    fn parse_value(&mut self) -> io::Result<Option<RespValue>> {
        if self.pos >= self.len {
            return Ok(None);
        }
        match self.buf[self.pos] {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected RESP type byte: 0x{:02x}", self.buf[self.pos]),
            )),
        }
    }

    fn read_line(&mut self) -> Option<&[u8]> {
        let start = self.pos + 1; // skip type byte
        let data = &self.buf[start..self.len];
        if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
            let end = start + pos;
            let result_start = start;
            let result_end = end;
            self.pos = end + 2; // skip \r\n
            Some(&self.buf[result_start..result_end])
        } else {
            None
        }
    }

    fn read_crlf_line(&mut self) -> Option<&[u8]> {
        let start = self.pos;
        let data = &self.buf[start..self.len];
        if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
            let end = start + pos;
            self.pos = end + 2;
            Some(&self.buf[start..end])
        } else {
            None
        }
    }

    fn parse_simple_string(&mut self) -> io::Result<Option<RespValue>> {
        let save = self.pos;
        match self.read_line() {
            Some(line) => Ok(Some(RespValue::SimpleString(
                String::from_utf8_lossy(line).into_owned(),
            ))),
            None => {
                self.pos = save;
                Ok(None)
            }
        }
    }

    fn parse_error(&mut self) -> io::Result<Option<RespValue>> {
        let save = self.pos;
        match self.read_line() {
            Some(line) => Ok(Some(RespValue::Error(
                String::from_utf8_lossy(line).into_owned(),
            ))),
            None => {
                self.pos = save;
                Ok(None)
            }
        }
    }

    fn parse_integer(&mut self) -> io::Result<Option<RespValue>> {
        let save = self.pos;
        match self.read_line() {
            Some(line) => {
                let s = std::str::from_utf8(line).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 integer")
                })?;
                let n: i64 = s.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid RESP integer")
                })?;
                Ok(Some(RespValue::Integer(n)))
            }
            None => {
                self.pos = save;
                Ok(None)
            }
        }
    }

    fn parse_bulk_string(&mut self) -> io::Result<Option<RespValue>> {
        let save = self.pos;
        let len_line = match self.read_line() {
            Some(l) => l.to_vec(),
            None => {
                self.pos = save;
                return Ok(None);
            }
        };
        let s = std::str::from_utf8(&len_line).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 bulk string length")
        })?;
        let n: i64 = s.parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid bulk string length")
        })?;
        if n == -1 {
            return Ok(Some(RespValue::BulkString(None)));
        }
        if n < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "negative bulk string length",
            ));
        }
        let n = n as usize;
        if self.pos + n + 2 > self.len {
            self.pos = save;
            return Ok(None);
        }
        let data = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n + 2; // skip \r\n
        Ok(Some(RespValue::BulkString(Some(data))))
    }

    fn parse_array(&mut self) -> io::Result<Option<RespValue>> {
        let save = self.pos;
        let len_line = match self.read_line() {
            Some(l) => l.to_vec(),
            None => {
                self.pos = save;
                return Ok(None);
            }
        };
        let s = std::str::from_utf8(&len_line).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 array length")
        })?;
        let n: i64 = s.parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid array length")
        })?;
        if n == -1 {
            return Ok(Some(RespValue::Array(None)));
        }
        if n < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "negative array length",
            ));
        }
        let n = n as usize;
        let mut items = Vec::with_capacity(n);
        for _ in 0..n {
            match self.parse_value()? {
                Some(v) => items.push(v),
                None => {
                    self.pos = save;
                    return Ok(None);
                }
            }
        }
        Ok(Some(RespValue::Array(Some(items))))
    }
}

// ─── Command handler ──────────────────────────────────────────────────────────

pub struct RedisHandler {
    pub db_manager: Arc<DatabaseManager>,
    current_db: Cell<u8>,
    /// Most recent write position (unused — kept for compatibility).
    pub last_wal_position: Cell<u64>,
    pub shard_hint: Cell<usize>,
}

impl RedisHandler {
    pub fn new(db_manager: Arc<DatabaseManager>) -> Self {
        RedisHandler {
            db_manager,
            current_db: Cell::new(0),
            last_wal_position: Cell::new(0),
            shard_hint: Cell::new(0),
        }
    }

    fn db(&self) -> &DbHandler {
        self.db_manager
            .db(self.current_db.get())
            .unwrap_or_else(|| self.db_manager.db(0).unwrap())
    }

    /// Dispatch a parsed RESP command and write the response into `out`.
    pub fn handle_command(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.is_empty() {
            RespValue::err("empty command").serialize_into(out);
            return;
        }
        let cmd = match &args[0] {
            RespValue::BulkString(Some(b)) => b.clone(),
            _ => {
                RespValue::err("invalid command").serialize_into(out);
                return;
            }
        };
        let cmd_upper: Vec<u8> = cmd.iter().map(|b| b.to_ascii_uppercase()).collect();

        match cmd_upper.as_slice() {
            b"PING" => self.cmd_ping(args, out),
            b"GET" => self.cmd_get(args, out),
            b"SET" => self.cmd_set(args, out),
            b"DEL" => self.cmd_del(args, out),
            b"EXISTS" => self.cmd_exists(args, out),
            b"MGET" => self.cmd_mget(args, out),
            b"MSET" => self.cmd_mset(args, out),
            b"KEYS" => self.cmd_keys(args, out),
            b"SCAN" => self.cmd_scan(args, out),
            b"DBSIZE" => self.cmd_dbsize(out),
            b"SELECT" => self.cmd_select(args, out),
            b"FLUSHDB" => self.cmd_flushdb(out),
            b"FLUSHALL" => self.cmd_flushall(out),
            b"INFO" => self.cmd_info(args, out),
            b"COMMAND" => self.cmd_command(out),
            b"QUIT" | b"RESET" => RespValue::ok().serialize_into(out),
            b"SETEX" | b"PSETEX" => self.cmd_setex(args, out),
            b"SETNX" => self.cmd_setnx(args, out),
            b"GETSET" => self.cmd_getset(args, out),
            b"APPEND" => self.cmd_append(args, out),
            b"STRLEN" => self.cmd_strlen(args, out),
            b"INCR" => self.cmd_incr(args, out, 1),
            b"DECR" => self.cmd_incr(args, out, -1),
            b"INCRBY" => self.cmd_incrby(args, out),
            b"DECRBY" => self.cmd_decrby(args, out),
            b"TTL" | b"PTTL" => RespValue::Integer(-1).serialize_into(out),
            b"EXPIRE" | b"PEXPIRE" | b"EXPIREAT" | b"PERSIST" => {
                RespValue::Integer(0).serialize_into(out)
            }
            b"TYPE" => self.cmd_type(args, out),
            b"OBJECT" => RespValue::null().serialize_into(out),
            b"RANDOMKEY" => self.cmd_randomkey(out),
            b"RENAME" => self.cmd_rename(args, out),
            b"RENAMENX" => self.cmd_renamenx(args, out),
            b"WAIT" | b"DEBUG" => RespValue::ok().serialize_into(out),
            b"BGSAVE" | b"BGREWRITEAOF" | b"SAVE" => {
                let _ = self.db().flush();
                RespValue::ok().serialize_into(out)
            }
            b"LASTSAVE" => RespValue::Integer(0).serialize_into(out),
            b"CLUSTER" => {
                RespValue::err("CLUSTER not supported").serialize_into(out);
            }
            b"REPLICAOF" | b"SLAVEOF" => RespValue::ok().serialize_into(out),
            b"CONFIG" => self.cmd_config(args, out),
            b"CLIENT" => RespValue::ok().serialize_into(out),
            b"SUBSCRIBE" | b"PSUBSCRIBE" | b"UNSUBSCRIBE" | b"PUNSUBSCRIBE" | b"PUBLISH" => {
                RespValue::err("Pub/Sub not supported").serialize_into(out);
            }
            b"MULTI" | b"EXEC" | b"DISCARD" | b"WATCH" | b"UNWATCH" => {
                RespValue::err("Transactions not supported").serialize_into(out);
            }
            b"LOLWUT" => {
                RespValue::bulk(b"SIndex KV Engine".to_vec()).serialize_into(out);
            }
            _ => {
                RespValue::Error(format!(
                    "ERR unknown command '{}', with args beginning with: ",
                    String::from_utf8_lossy(&cmd)
                ))
                .serialize_into(out);
            }
        }
    }

    pub fn take_wal_position(&self) -> u64 {
        self.last_wal_position.replace(0)
    }

    // ─── Individual command implementations ──────────────────────────────────

    fn cmd_ping(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() > 1 {
            if let RespValue::BulkString(Some(msg)) = &args[1] {
                RespValue::BulkString(Some(msg.clone())).serialize_into(out);
                return;
            }
        }
        RespValue::pong().serialize_into(out);
    }

    fn cmd_get(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("wrong number of arguments for GET").serialize_into(out);
            return;
        }
        let key = bulk_bytes(&args[1]);
        match self.db().get_value(key) {
            Some(v) => RespValue::bulk(v).serialize_into(out),
            None => RespValue::null().serialize_into(out),
        }
    }

    fn cmd_set(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("wrong number of arguments for SET").serialize_into(out);
            return;
        }
        let key = bulk_bytes(&args[1]);
        let value = bulk_bytes(&args[2]);

        // Parse optional SET options: EX, PX, NX, XX, GET
        let mut ttl = 0u32;
        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        let mut i = 3;
        while i < args.len() {
            let opt = bulk_bytes_upper(&args[i]);
            match opt.as_slice() {
                b"EX" if i + 1 < args.len() => {
                    if let Ok(n) = std::str::from_utf8(bulk_bytes(&args[i + 1]))
                        .unwrap_or("")
                        .parse::<u32>()
                    {
                        ttl = n;
                    }
                    i += 2;
                }
                b"PX" if i + 1 < args.len() => {
                    if let Ok(n) = std::str::from_utf8(bulk_bytes(&args[i + 1]))
                        .unwrap_or("")
                        .parse::<u64>()
                    {
                        ttl = (n / 1000) as u32;
                    }
                    i += 2;
                }
                b"NX" => {
                    nx = true;
                    i += 1;
                }
                b"XX" => {
                    xx = true;
                    i += 1;
                }
                b"GET" => {
                    get = true;
                    i += 1;
                }
                b"KEEPTTL" => {
                    i += 1;
                }
                b"EXAT" | b"PXAT" => {
                    i += 2; // skip value
                }
                _ => i += 1,
            }
        }

        let old_value = if get { self.db().get_value(key) } else { None };

        if nx && self.db().get_value(key).is_some() {
            if get {
                match old_value {
                    Some(v) => RespValue::bulk(v).serialize_into(out),
                    None => RespValue::null().serialize_into(out),
                }
            } else {
                RespValue::null().serialize_into(out);
            }
            return;
        }
        if xx && self.db().get_value(key).is_none() {
            if get {
                RespValue::null().serialize_into(out);
            } else {
                RespValue::null().serialize_into(out);
            }
            return;
        }

        match self.db().put_nowait_on(self.shard_hint.get(), key, value, ttl) {
            Ok(_) => {
                if get {
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

    fn cmd_setex(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 4 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let key = bulk_bytes(&args[1]);
        let value = bulk_bytes(&args[3]);
        match self
            .db()
            .put_nowait_on(self.shard_hint.get(), key, value, 0)
        {
            Ok(_) => RespValue::ok().serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    fn cmd_setnx(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let key = bulk_bytes(&args[1]);
        let value = bulk_bytes(&args[2]);
        if self.db().get_value(key).is_some() {
            RespValue::Integer(0).serialize_into(out);
            return;
        }
        match self.db().put_nowait_on(self.shard_hint.get(), key, value, 0) {
            Ok(_) => RespValue::Integer(1).serialize_into(out),
            Err(e) => RespValue::err(&e.to_string()).serialize_into(out),
        }
    }

    fn cmd_getset(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let key = bulk_bytes(&args[1]);
        let old = self.db().get_value(key);
        let value = bulk_bytes(&args[2]);
        let _ = self.db().put_nowait_on(self.shard_hint.get(), key, value, 0);
        match old {
            Some(v) => RespValue::bulk(v).serialize_into(out),
            None => RespValue::null().serialize_into(out),
        }
    }

    fn cmd_append(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let key = bulk_bytes(&args[1]);
        let extra = bulk_bytes(&args[2]);
        let mut current = self.db().get_value(key).unwrap_or_default();
        current.extend_from_slice(extra);
        let new_len = current.len();
        let _ = self.db().put_nowait_on(self.shard_hint.get(), key, &current, 0);
        RespValue::Integer(new_len as i64).serialize_into(out);
    }

    fn cmd_strlen(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let key = bulk_bytes(&args[1]);
        let len = self.db().get_value(key).map_or(0, |v| v.len());
        RespValue::Integer(len as i64).serialize_into(out);
    }

    fn cmd_incr(&self, args: &[RespValue], out: &mut Vec<u8>, delta: i64) {
        if args.len() < 2 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let key = bulk_bytes(&args[1]);
        let current = self
            .db()
            .get_value(key)
            .and_then(|v| std::str::from_utf8(&v).ok().and_then(|s| s.parse::<i64>().ok()))
            .unwrap_or(0);
        let new_val = current + delta;
        let new_str = new_val.to_string();
        let _ = self
            .db()
            .put_nowait_on(self.shard_hint.get(), key, new_str.as_bytes(), 0);
        RespValue::Integer(new_val).serialize_into(out);
    }

    fn cmd_incrby(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let delta: i64 = std::str::from_utf8(bulk_bytes(&args[2]))
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        self.cmd_incr(args, out, delta);
    }

    fn cmd_decrby(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let delta: i64 = std::str::from_utf8(bulk_bytes(&args[2]))
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        self.cmd_incr(args, out, -delta);
    }

    fn cmd_del(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("wrong number of arguments for DEL").serialize_into(out);
            return;
        }
        let mut count = 0i64;
        for key_arg in &args[1..] {
            let key = bulk_bytes(key_arg);
            match self
                .db()
                .delete_nowait_on(self.shard_hint.get(), key)
            {
                Ok((true, _)) => count += 1,
                _ => {}
            }
        }
        RespValue::Integer(count).serialize_into(out);
    }

    fn cmd_exists(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("wrong number of arguments for EXISTS").serialize_into(out);
            return;
        }
        let mut count = 0i64;
        for key_arg in &args[1..] {
            let key = bulk_bytes(key_arg);
            if self.db().get_value(key).is_some() {
                count += 1;
            }
        }
        RespValue::Integer(count).serialize_into(out);
    }

    fn cmd_mget(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("wrong number of arguments for MGET").serialize_into(out);
            return;
        }
        let items: Vec<RespValue> = args[1..]
            .iter()
            .map(|a| match self.db().get_value(bulk_bytes(a)) {
                Some(v) => RespValue::bulk(v),
                None => RespValue::null(),
            })
            .collect();
        RespValue::Array(Some(items)).serialize_into(out);
    }

    fn cmd_mset(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 || args.len() % 2 == 0 {
            RespValue::err("wrong number of arguments for MSET").serialize_into(out);
            return;
        }
        let mut i = 1;
        while i + 1 < args.len() {
            let key = bulk_bytes(&args[i]);
            let value = bulk_bytes(&args[i + 1]);
            let _ = self.db().put_sync(key, value, 0);
            i += 2;
        }
        RespValue::ok().serialize_into(out);
    }

    fn cmd_type(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let key = bulk_bytes(&args[1]);
        if self.db().get_value(key).is_some() {
            RespValue::SimpleString("string".to_string()).serialize_into(out);
        } else {
            RespValue::SimpleString("none".to_string()).serialize_into(out);
        }
    }

    fn cmd_keys(&self, args: &[RespValue], out: &mut Vec<u8>) {
        let pattern = if args.len() > 1 {
            Some(bulk_bytes(&args[1]).to_vec())
        } else {
            None
        };
        let mut keys: Vec<RespValue> = Vec::new();
        self.db().iter_keys(|k| {
            if let Some(ref pat) = pattern {
                if !glob_match(pat, k) {
                    return;
                }
            }
            keys.push(RespValue::bulk(k.to_vec()));
        });
        RespValue::Array(Some(keys)).serialize_into(out);
    }

    fn cmd_scan(&self, args: &[RespValue], out: &mut Vec<u8>) {
        let cursor: u64 = if args.len() > 1 {
            std::str::from_utf8(bulk_bytes(&args[1]))
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0)
        } else {
            0
        };

        let mut count = 10usize;
        let mut pattern: Option<Vec<u8>> = None;
        let mut i = 2;
        while i + 1 < args.len() {
            let opt = bulk_bytes_upper(&args[i]);
            match opt.as_slice() {
                b"COUNT" => {
                    count = std::str::from_utf8(bulk_bytes(&args[i + 1]))
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(10);
                    i += 2;
                }
                b"MATCH" => {
                    pattern = Some(bulk_bytes(&args[i + 1]).to_vec());
                    i += 2;
                }
                b"TYPE" => {
                    i += 2; // ignore TYPE filter (all entries are "string")
                }
                _ => i += 1,
            }
        }

        let mut results = Vec::new();
        let next_cursor = self.db().scan_keys(cursor, count, pattern.as_deref(), &mut results);

        let items: Vec<RespValue> = results.into_iter().map(RespValue::bulk).collect();
        RespValue::Array(Some(vec![
            RespValue::bulk(next_cursor.to_string().into_bytes()),
            RespValue::Array(Some(items)),
        ]))
        .serialize_into(out);
    }

    fn cmd_dbsize(&self, out: &mut Vec<u8>) {
        RespValue::Integer(self.db().live_entries() as i64).serialize_into(out);
    }

    fn cmd_select(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::err("wrong number of arguments for SELECT").serialize_into(out);
            return;
        }
        let idx: u8 = std::str::from_utf8(bulk_bytes(&args[1]))
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(255);
        if self.db_manager.db(idx).is_some() {
            self.current_db.set(idx);
            RespValue::ok().serialize_into(out);
        } else {
            RespValue::err("DB index is out of range").serialize_into(out);
        }
    }

    fn cmd_flushdb(&self, out: &mut Vec<u8>) {
        self.db().clear();
        RespValue::ok().serialize_into(out);
    }

    fn cmd_flushall(&self, out: &mut Vec<u8>) {
        for i in 0..self.db_manager.num_dbs() {
            if let Some(db) = self.db_manager.db(i) {
                db.clear();
            }
        }
        RespValue::ok().serialize_into(out);
    }

    fn cmd_randomkey(&self, out: &mut Vec<u8>) {
        match self.db().random_key() {
            Some(k) => RespValue::bulk(k).serialize_into(out),
            None => RespValue::null().serialize_into(out),
        }
    }

    fn cmd_rename(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let src = bulk_bytes(&args[1]);
        let dst = bulk_bytes(&args[2]);
        match self.db().get_value(src) {
            None => RespValue::err("no such key").serialize_into(out),
            Some(v) => {
                let _ = self.db().put_sync(dst, &v, 0);
                let _ = self.db().delete_sync(src);
                RespValue::ok().serialize_into(out);
            }
        }
    }

    fn cmd_renamenx(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 3 {
            RespValue::err("wrong number of arguments").serialize_into(out);
            return;
        }
        let src = bulk_bytes(&args[1]);
        let dst = bulk_bytes(&args[2]);
        if self.db().get_value(dst).is_some() {
            RespValue::Integer(0).serialize_into(out);
            return;
        }
        match self.db().get_value(src) {
            None => RespValue::err("no such key").serialize_into(out),
            Some(v) => {
                let _ = self.db().put_sync(dst, &v, 0);
                let _ = self.db().delete_sync(src);
                RespValue::Integer(1).serialize_into(out);
            }
        }
    }

    fn cmd_info(&self, args: &[RespValue], out: &mut Vec<u8>) {
        let section = if args.len() > 1 {
            String::from_utf8_lossy(bulk_bytes(&args[1]))
                .to_lowercase()
        } else {
            "all".to_string()
        };

        let db0 = self.db_manager.db(0);
        let entries = db0.map_or(0, |d| d.live_entries());
        let data_bytes = db0.map_or(0, |d| d.total_data_bytes());

        let info = format!(
            "# Server\r\n\
             redis_version:7.0.0-sindex\r\n\
             redis_mode:standalone\r\n\
             os:Linux\r\n\
             arch_bits:64\r\n\
             tcp_port:6379\r\n\
             \r\n\
             # Clients\r\n\
             connected_clients:1\r\n\
             \r\n\
             # Memory\r\n\
             used_memory:{data_bytes}\r\n\
             \r\n\
             # Stats\r\n\
             total_commands_processed:0\r\n\
             \r\n\
             # Replication\r\n\
             role:master\r\n\
             connected_slaves:0\r\n\
             \r\n\
             # Keyspace\r\n\
             db0:keys={entries},expires=0,avg_ttl=0\r\n\
             \r\n\
             # SIndex\r\n\
             index_type:sindex-btree\r\n\
             partition_bits:16\r\n\
             btree_max_height:4\r\n\
             paper:10.1145/3789205\r\n\
             ",
            data_bytes = data_bytes,
            entries = entries,
        );

        RespValue::bulk(info.into_bytes()).serialize_into(out);
    }

    fn cmd_command(&self, out: &mut Vec<u8>) {
        // Return an empty array for COMMAND (clients use it as a no-op probe)
        RespValue::Array(Some(vec![])).serialize_into(out);
    }

    fn cmd_config(&self, args: &[RespValue], out: &mut Vec<u8>) {
        if args.len() < 2 {
            RespValue::ok().serialize_into(out);
            return;
        }
        let sub = bulk_bytes_upper(&args[1]);
        match sub.as_slice() {
            b"GET" => {
                // Return empty config
                RespValue::Array(Some(vec![])).serialize_into(out);
            }
            b"SET" => RespValue::ok().serialize_into(out),
            b"RESETSTAT" => RespValue::ok().serialize_into(out),
            _ => RespValue::ok().serialize_into(out),
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn bulk_bytes<'a>(v: &'a RespValue) -> &'a [u8] {
    match v {
        RespValue::BulkString(Some(b)) => b,
        RespValue::SimpleString(s) => s.as_bytes(),
        _ => b"",
    }
}

fn bulk_bytes_upper(v: &RespValue) -> Vec<u8> {
    bulk_bytes(v).iter().map(|b| b.to_ascii_uppercase()).collect()
}

/// Glob matching: `*` = any sequence, `?` = one char.
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let (p, t) = (pattern, text);
    let (mut pi, mut ti, mut star_pi, mut star_ti) = (0usize, 0usize, usize::MAX, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            star_ti += 1;
            ti = star_ti;
            pi = star_pi + 1;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

/// Pub/sub manager stub (kept for API compatibility with reactor.rs).
pub struct PubSubManager;
impl PubSubManager {
    pub fn new() -> Self { PubSubManager }
}
