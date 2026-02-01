//! Pipelined request processing for higher throughput.
//!
//! Pipelining allows multiple requests to be sent on the same connection
//! without waiting for responses, then responses are sent back in order.
//! This amortizes network round-trip latency.

use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use tracing::{debug, trace};

use crate::server::handler::Handler;
use crate::server::protocol::{Header, Request, Response, HEADER_SIZE, PROTOCOL_MAGIC};

/// Maximum number of pipelined requests
const MAX_PIPELINE_DEPTH: usize = 128;

/// Pipelined connection handler
pub struct PipelinedConnection {
    stream: TcpStream,
    handler: Arc<Handler>,
    pending_responses: VecDeque<Response>,
    read_buf: Vec<u8>,
}

impl PipelinedConnection {
    pub fn new(stream: TcpStream, handler: Arc<Handler>) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        stream.set_nonblocking(false)?;

        Ok(Self {
            stream,
            handler,
            pending_responses: VecDeque::with_capacity(MAX_PIPELINE_DEPTH),
            read_buf: vec![0u8; 64 * 1024], // 64KB read buffer
        })
    }

    /// Process requests with pipelining support
    pub fn run(&mut self) -> io::Result<()> {
        let mut reader = BufReader::with_capacity(64 * 1024, self.stream.try_clone()?);
        let mut header_buf = [0u8; HEADER_SIZE];

        loop {
            // Try to read a request header
            match reader.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Client disconnected
                    self.flush_responses()?;
                    return Ok(());
                }
                Err(e) => return Err(e),
            }

            // Parse header
            let header = Header::from_bytes(&header_buf);
            if !header.is_valid() {
                debug!("Invalid header");
                continue;
            }

            // Read payload if present
            let payload = if header.payload_len > 0 {
                let mut payload = vec![0u8; header.payload_len as usize];
                reader.read_exact(&mut payload)?;
                payload
            } else {
                Vec::new()
            };

            // Create request
            let request = Request { header, payload };

            // Process request immediately (could batch for even more perf)
            let response = self.process_request(request);
            self.pending_responses.push_back(response);

            // Flush responses if pipeline is full or no more data ready
            if self.pending_responses.len() >= MAX_PIPELINE_DEPTH {
                self.flush_responses()?;
            } else if !self.has_pending_data(&mut reader)? {
                // No more data waiting, flush what we have
                self.flush_responses()?;
            }
        }
    }

    /// Check if there's more data ready to read
    fn has_pending_data<R: BufRead>(&self, reader: &mut R) -> io::Result<bool> {
        // Check if buffer has data
        Ok(reader.fill_buf()?.len() > 0)
    }

    /// Process a single request
    fn process_request(&self, request: Request) -> Response {
        let request_id = request.header.request_id;

        // Use synchronous processing for pipelining
        match request.header.opcode {
            crate::server::protocol::Opcode::Get => {
                match request.parse_get() {
                    Ok(key) => {
                        match self.handler.get_value(&key) {
                            Some(value) => Response::success(request_id, &value),
                            None => Response::not_found(request_id),
                        }
                    }
                    Err(e) => Response::invalid(request_id, &e.to_string()),
                }
            }
            crate::server::protocol::Opcode::Put => {
                match request.parse_put() {
                    Ok((key, value, ttl)) => {
                        match self.handler.put_sync(&key, &value, ttl) {
                            Ok(_) => Response::success(request_id, &[]),
                            Err(e) => Response::error(request_id, &e.to_string()),
                        }
                    }
                    Err(e) => Response::invalid(request_id, &e.to_string()),
                }
            }
            crate::server::protocol::Opcode::Delete => {
                match request.parse_delete() {
                    Ok(key) => {
                        match self.handler.delete_sync(&key) {
                            Ok(_) => Response::success(request_id, &[]),
                            Err(e) => Response::error(request_id, &e.to_string()),
                        }
                    }
                    Err(e) => Response::invalid(request_id, &e.to_string()),
                }
            }
            crate::server::protocol::Opcode::Ping => Response::pong(request_id),
            _ => Response::invalid(request_id, "Unknown opcode"),
        }
    }

    /// Flush all pending responses
    fn flush_responses(&mut self) -> io::Result<()> {
        // Batch serialize all responses
        let mut buf = Vec::with_capacity(self.pending_responses.len() * 64);

        while let Some(response) = self.pending_responses.pop_front() {
            buf.extend_from_slice(&response.serialize());
        }

        if !buf.is_empty() {
            self.stream.write_all(&buf)?;
            self.stream.flush()?;
        }

        Ok(())
    }
}

/// Batch request for multiple operations in one round-trip
#[derive(Debug)]
pub struct BatchRequest {
    pub operations: Vec<BatchOperation>,
}

#[derive(Debug)]
pub enum BatchOperation {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8>, ttl: u32 },
    Delete { key: Vec<u8> },
}

#[derive(Debug)]
pub struct BatchResponse {
    pub results: Vec<BatchResult>,
}

#[derive(Debug)]
pub enum BatchResult {
    Value(Option<Vec<u8>>),
    Ok,
    Error(String),
}

impl BatchRequest {
    /// Execute batch operations
    pub fn execute(&self, handler: &Handler) -> BatchResponse {
        let mut results = Vec::with_capacity(self.operations.len());

        for op in &self.operations {
            let result = match op {
                BatchOperation::Get { key } => {
                    BatchResult::Value(handler.get_value(key))
                }
                BatchOperation::Put { key, value, ttl } => {
                    match handler.put_sync(key, value, *ttl) {
                        Ok(_) => BatchResult::Ok,
                        Err(e) => BatchResult::Error(e.to_string()),
                    }
                }
                BatchOperation::Delete { key } => {
                    match handler.delete_sync(key) {
                        Ok(_) => BatchResult::Ok,
                        Err(e) => BatchResult::Error(e.to_string()),
                    }
                }
            };
            results.push(result);
        }

        BatchResponse { results }
    }
}
