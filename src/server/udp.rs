//! High-performance UDP server for read operations.
//!
//! UDP eliminates TCP overhead:
//! - No connection state
//! - No ACKs/retransmissions
//! - No Nagle's algorithm
//! - Smaller headers (8 bytes vs 20+ for TCP)

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tracing::{debug, error, info, trace};

use crate::server::handler::Handler;
use crate::server::protocol::PROTOCOL_MAGIC;

/// Type alias for clarity
type RequestHandler = Handler;

/// UDP packet format (simplified for speed):
/// [2: magic] [1: opcode] [1: key_len] [4: request_id] [key...]
///
/// Response format:
/// [2: magic] [1: status] [1: reserved] [4: request_id] [4: value_len] [value...]

const UDP_HEADER_SIZE: usize = 8;
const MAX_UDP_PACKET: usize = 65507; // Max UDP payload
const BATCH_HEADER_SIZE: usize = 12; // magic(2) + opcode(1) + count(1) + request_id(4) + reserved(4)

pub struct UdpServer {
    socket: UdpSocket,
    handler: Arc<RequestHandler>,
    recv_buf: Vec<u8>,
    send_buf: Vec<u8>,
}

impl UdpServer {
    pub async fn new(addr: SocketAddr, handler: Arc<RequestHandler>) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;

        // Increase buffer sizes for high throughput
        socket.set_broadcast(false)?;

        info!("UDP server listening on {}", addr);

        Ok(Self {
            socket,
            handler,
            recv_buf: vec![0u8; MAX_UDP_PACKET],
            send_buf: vec![0u8; MAX_UDP_PACKET],
        })
    }

    pub async fn run(&mut self) -> io::Result<()> {
        // Temporary buffer for packet processing
        let mut packet_buf = vec![0u8; MAX_UDP_PACKET];

        loop {
            let (len, src) = self.socket.recv_from(&mut self.recv_buf).await?;

            if len < UDP_HEADER_SIZE {
                continue; // Too short, ignore
            }

            // Copy packet data to avoid borrow conflict
            packet_buf[..len].copy_from_slice(&self.recv_buf[..len]);

            let response_len = self.process_packet(&packet_buf[..len]);

            if response_len > 0 {
                // Send response - ignore errors (UDP is best-effort)
                let _ = self.socket.send_to(&self.send_buf[..response_len], src).await;
            }
        }
    }

    fn process_packet(&mut self, packet: &[u8]) -> usize {
        // Check magic
        let magic = u16::from_le_bytes([packet[0], packet[1]]);
        if magic != PROTOCOL_MAGIC {
            return 0;
        }

        let opcode = packet[2];

        match opcode {
            0x01 => self.handle_get(packet),      // Single GET
            0x11 => self.handle_multi_get(packet), // Batch GET
            0x20 => self.handle_ping(packet),      // PING
            _ => 0, // Unknown opcode
        }
    }

    fn handle_get(&mut self, packet: &[u8]) -> usize {
        if packet.len() < UDP_HEADER_SIZE {
            return 0;
        }

        let key_len = packet[3] as usize;
        let request_id = u32::from_le_bytes([packet[4], packet[5], packet[6], packet[7]]);

        if packet.len() < UDP_HEADER_SIZE + key_len {
            return 0;
        }

        let key = &packet[UDP_HEADER_SIZE..UDP_HEADER_SIZE + key_len];

        // Fast path: direct index lookup
        match self.handler.get_value(key) {
            Some(value) => {
                self.build_response(request_id, 0x00, Some(&value))
            }
            None => {
                self.build_response(request_id, 0x01, None) // NOT_FOUND
            }
        }
    }

    fn handle_multi_get(&mut self, packet: &[u8]) -> usize {
        if packet.len() < BATCH_HEADER_SIZE {
            return 0;
        }

        let count = packet[3] as usize;
        let request_id = u32::from_le_bytes([packet[4], packet[5], packet[6], packet[7]]);

        let mut offset = BATCH_HEADER_SIZE;
        let mut response_offset = 12; // Response header

        // Write response header
        self.send_buf[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
        self.send_buf[2] = 0x00; // Success
        self.send_buf[3] = count as u8;
        self.send_buf[4..8].copy_from_slice(&request_id.to_le_bytes());

        for _ in 0..count {
            if offset >= packet.len() {
                break;
            }

            let key_len = packet[offset] as usize;
            offset += 1;

            if offset + key_len > packet.len() {
                break;
            }

            let key = &packet[offset..offset + key_len];
            offset += key_len;

            // Lookup and append to response
            match self.handler.get_value(key) {
                Some(value) => {
                    if response_offset + 5 + value.len() > MAX_UDP_PACKET {
                        break; // Response too large
                    }
                    self.send_buf[response_offset] = 0x00; // Found
                    let vlen = (value.len() as u32).to_le_bytes();
                    self.send_buf[response_offset + 1..response_offset + 5].copy_from_slice(&vlen);
                    self.send_buf[response_offset + 5..response_offset + 5 + value.len()]
                        .copy_from_slice(&value);
                    response_offset += 5 + value.len();
                }
                None => {
                    self.send_buf[response_offset] = 0x01; // Not found
                    response_offset += 1;
                }
            }
        }

        response_offset
    }

    fn handle_ping(&mut self, packet: &[u8]) -> usize {
        if packet.len() < UDP_HEADER_SIZE {
            return 0;
        }
        let request_id = u32::from_le_bytes([packet[4], packet[5], packet[6], packet[7]]);
        self.build_response(request_id, 0x00, Some(b"PONG"))
    }

    fn build_response(&mut self, request_id: u32, status: u8, value: Option<&[u8]>) -> usize {
        self.send_buf[0..2].copy_from_slice(&PROTOCOL_MAGIC.to_le_bytes());
        self.send_buf[2] = status;
        self.send_buf[3] = 0; // Reserved
        self.send_buf[4..8].copy_from_slice(&request_id.to_le_bytes());

        match value {
            Some(v) => {
                let vlen = (v.len() as u32).to_le_bytes();
                self.send_buf[8..12].copy_from_slice(&vlen);
                self.send_buf[12..12 + v.len()].copy_from_slice(v);
                12 + v.len()
            }
            None => {
                self.send_buf[8..12].copy_from_slice(&0u32.to_le_bytes());
                12
            }
        }
    }
}

/// Starts the UDP server on a separate task.
pub fn start_udp_server(
    addr: SocketAddr,
    handler: Arc<RequestHandler>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match UdpServer::new(addr, handler).await {
            Ok(mut server) => {
                if let Err(e) = server.run().await {
                    error!("UDP server error: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to start UDP server: {}", e);
            }
        }
    })
}
