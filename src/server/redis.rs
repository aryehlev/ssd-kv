//! Redis protocol (RESP) support for compatibility with Redis clients.
//!
//! RESP format:
//! - Simple Strings: +OK\r\n
//! - Errors: -ERR message\r\n
//! - Integers: :1000\r\n
//! - Bulk Strings: $6\r\nfoobar\r\n (or $-1\r\n for null)
//! - Arrays: *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n

use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::Arc;

use tracing::{debug, error, info, trace};

use crate::server::handler::Handler;

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
    /// Serializes the value to RESP format
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RespValue::Error(s) => format!("-{}\r\n", s).into_bytes(),
            RespValue::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            RespValue::BulkString(None) => b"$-1\r\n".to_vec(),
            RespValue::BulkString(Some(data)) => {
                let mut buf = format!("${}\r\n", data.len()).into_bytes();
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
                buf
            }
            RespValue::Array(None) => b"*-1\r\n".to_vec(),
            RespValue::Array(Some(items)) => {
                let mut buf = format!("*{}\r\n", items.len()).into_bytes();
                for item in items {
                    buf.extend_from_slice(&item.serialize());
                }
                buf
            }
        }
    }

    /// Helper to create OK response
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    /// Helper to create PONG response
    pub fn pong() -> Self {
        RespValue::SimpleString("PONG".to_string())
    }

    /// Helper to create null bulk string
    pub fn null() -> Self {
        RespValue::BulkString(None)
    }

    /// Helper to create error
    pub fn err(msg: &str) -> Self {
        RespValue::Error(format!("ERR {}", msg))
    }
}

/// RESP parser
pub struct RespParser<R: BufRead> {
    reader: R,
}

impl<R: BufRead> RespParser<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Parses a single RESP value
    pub fn parse(&mut self) -> io::Result<Option<RespValue>> {
        let mut line = String::new();
        let bytes_read = self.reader.read_line(&mut line)?;

        if bytes_read == 0 {
            return Ok(None); // EOF
        }

        let line = line.trim_end_matches("\r\n").trim_end_matches('\n');

        if line.is_empty() {
            return Ok(None);
        }

        let type_char = line.chars().next().unwrap();
        let content = &line[1..];

        match type_char {
            '+' => Ok(Some(RespValue::SimpleString(content.to_string()))),
            '-' => Ok(Some(RespValue::Error(content.to_string()))),
            ':' => {
                let i = content.parse::<i64>()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid integer"))?;
                Ok(Some(RespValue::Integer(i)))
            }
            '$' => {
                let len = content.parse::<i64>()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk length"))?;

                if len < 0 {
                    return Ok(Some(RespValue::BulkString(None)));
                }

                let len = len as usize;
                let mut data = vec![0u8; len];
                self.reader.read_exact(&mut data)?;

                // Read trailing \r\n
                let mut crlf = [0u8; 2];
                self.reader.read_exact(&mut crlf)?;

                Ok(Some(RespValue::BulkString(Some(data))))
            }
            '*' => {
                let count = content.parse::<i64>()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid array length"))?;

                if count < 0 {
                    return Ok(Some(RespValue::Array(None)));
                }

                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    match self.parse()? {
                        Some(item) => items.push(item),
                        None => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Incomplete array")),
                    }
                }

                Ok(Some(RespValue::Array(Some(items))))
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, format!("Unknown RESP type: {}", type_char))),
        }
    }
}

/// Redis command handler
pub struct RedisHandler {
    handler: Arc<Handler>,
}

impl RedisHandler {
    pub fn new(handler: Arc<Handler>) -> Self {
        Self { handler }
    }

    /// Handles a Redis command and returns a response
    pub fn handle_command(&self, value: RespValue) -> RespValue {
        let args = match value {
            RespValue::Array(Some(items)) => items,
            _ => return RespValue::err("Expected array"),
        };

        if args.is_empty() {
            return RespValue::err("Empty command");
        }

        // Extract command name
        let cmd = match &args[0] {
            RespValue::BulkString(Some(data)) => {
                String::from_utf8_lossy(data).to_uppercase()
            }
            _ => return RespValue::err("Invalid command format"),
        };

        match cmd.as_str() {
            "PING" => self.cmd_ping(&args),
            "GET" => self.cmd_get(&args),
            "SET" => self.cmd_set(&args),
            "DEL" => self.cmd_del(&args),
            "MGET" => self.cmd_mget(&args),
            "MSET" => self.cmd_mset(&args),
            "EXISTS" => self.cmd_exists(&args),
            "COMMAND" => self.cmd_command(),
            "INFO" => self.cmd_info(),
            "DBSIZE" => self.cmd_dbsize(),
            _ => RespValue::err(&format!("Unknown command: {}", cmd)),
        }
    }

    fn cmd_ping(&self, args: &[RespValue]) -> RespValue {
        if args.len() > 1 {
            // PING with argument echoes it back
            if let RespValue::BulkString(Some(data)) = &args[1] {
                return RespValue::BulkString(Some(data.clone()));
            }
        }
        RespValue::pong()
    }

    fn cmd_get(&self, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::err("GET requires 1 argument");
        }

        let key = match &args[1] {
            RespValue::BulkString(Some(data)) => data,
            _ => return RespValue::err("Invalid key"),
        };

        match self.handler.get_value(key) {
            Some(value) => RespValue::BulkString(Some(value)),
            None => RespValue::null(),
        }
    }

    fn cmd_set(&self, args: &[RespValue]) -> RespValue {
        if args.len() < 3 {
            return RespValue::err("SET requires 2 arguments");
        }

        let key = match &args[1] {
            RespValue::BulkString(Some(data)) => data.clone(),
            _ => return RespValue::err("Invalid key"),
        };

        let value = match &args[2] {
            RespValue::BulkString(Some(data)) => data.clone(),
            _ => return RespValue::err("Invalid value"),
        };

        // Parse optional EX/PX for TTL
        let ttl = self.parse_set_options(&args[3..]);

        // Use the handler to store the value
        match self.handler.put_sync(&key, &value, ttl) {
            Ok(_) => RespValue::ok(),
            Err(e) => RespValue::err(&e.to_string()),
        }
    }

    fn parse_set_options(&self, args: &[RespValue]) -> u32 {
        let mut i = 0;
        while i < args.len() {
            if let RespValue::BulkString(Some(opt)) = &args[i] {
                let opt_str = String::from_utf8_lossy(opt).to_uppercase();
                match opt_str.as_str() {
                    "EX" => {
                        // Seconds
                        if i + 1 < args.len() {
                            if let RespValue::BulkString(Some(val)) = &args[i + 1] {
                                if let Ok(secs) = String::from_utf8_lossy(val).parse::<u32>() {
                                    return secs;
                                }
                            }
                        }
                    }
                    "PX" => {
                        // Milliseconds -> convert to seconds
                        if i + 1 < args.len() {
                            if let RespValue::BulkString(Some(val)) = &args[i + 1] {
                                if let Ok(ms) = String::from_utf8_lossy(val).parse::<u32>() {
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
        0 // No TTL
    }

    fn cmd_del(&self, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::err("DEL requires at least 1 argument");
        }

        let mut deleted = 0i64;
        for arg in &args[1..] {
            if let RespValue::BulkString(Some(key)) = arg {
                if self.handler.delete_sync(key).is_ok() {
                    deleted += 1;
                }
            }
        }

        RespValue::Integer(deleted)
    }

    fn cmd_mget(&self, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::err("MGET requires at least 1 argument");
        }

        let mut results = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            if let RespValue::BulkString(Some(key)) = arg {
                match self.handler.get_value(key) {
                    Some(value) => results.push(RespValue::BulkString(Some(value))),
                    None => results.push(RespValue::null()),
                }
            } else {
                results.push(RespValue::null());
            }
        }

        RespValue::Array(Some(results))
    }

    fn cmd_mset(&self, args: &[RespValue]) -> RespValue {
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return RespValue::err("MSET requires key-value pairs");
        }

        let mut i = 1;
        while i + 1 < args.len() {
            let key = match &args[i] {
                RespValue::BulkString(Some(data)) => data.clone(),
                _ => return RespValue::err("Invalid key"),
            };
            let value = match &args[i + 1] {
                RespValue::BulkString(Some(data)) => data.clone(),
                _ => return RespValue::err("Invalid value"),
            };

            if let Err(e) = self.handler.put_sync(&key, &value, 0) {
                return RespValue::err(&e.to_string());
            }
            i += 2;
        }

        RespValue::ok()
    }

    fn cmd_exists(&self, args: &[RespValue]) -> RespValue {
        if args.len() < 2 {
            return RespValue::err("EXISTS requires at least 1 argument");
        }

        let mut count = 0i64;
        for arg in &args[1..] {
            if let RespValue::BulkString(Some(key)) = arg {
                if self.handler.get_value(key).is_some() {
                    count += 1;
                }
            }
        }

        RespValue::Integer(count)
    }

    fn cmd_command(&self) -> RespValue {
        // Minimal COMMAND response for client compatibility
        RespValue::Array(Some(vec![]))
    }

    fn cmd_info(&self) -> RespValue {
        let info = "# Server\r\nredis_version:7.0.0-ssd-kv\r\n# Keyspace\r\n";
        RespValue::BulkString(Some(info.as_bytes().to_vec()))
    }

    fn cmd_dbsize(&self) -> RespValue {
        // Return index size
        RespValue::Integer(0) // TODO: get actual count from index
    }
}

/// Redis server that listens for connections
pub struct RedisServer {
    addr: SocketAddr,
    handler: Arc<Handler>,
}

impl RedisServer {
    pub fn new(addr: SocketAddr, handler: Arc<Handler>) -> Self {
        Self { addr, handler }
    }

    /// Runs the Redis server (blocking)
    pub fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(self.addr)?;
        info!("Redis server listening on {}", self.addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let handler = Arc::clone(&self.handler);
                    std::thread::spawn(move || {
                        if let Err(e) = handle_redis_client(stream, handler) {
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

/// Handles a single Redis client connection
fn handle_redis_client(stream: TcpStream, handler: Arc<Handler>) -> io::Result<()> {
    stream.set_nodelay(true)?;

    let peer = stream.peer_addr()?;
    debug!("Redis client connected: {}", peer);

    let reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    let mut parser = RespParser::new(reader);
    let redis_handler = RedisHandler::new(handler);

    loop {
        match parser.parse() {
            Ok(Some(value)) => {
                trace!("Redis command: {:?}", value);
                let response = redis_handler.handle_command(value);
                let data = response.serialize();
                writer.write_all(&data)?;
                writer.flush()?;
            }
            Ok(None) => {
                debug!("Redis client disconnected: {}", peer);
                break;
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                }
                error!("Parse error: {}", e);
                let err = RespValue::err(&e.to_string());
                writer.write_all(&err.serialize())?;
            }
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
