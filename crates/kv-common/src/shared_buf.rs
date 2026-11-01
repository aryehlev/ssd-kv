//! Reference-counted reply bodies (DESIGN §7.1, §15).
//!
//! Values of 64 KiB or more are passed between workers and to the socket writer as shared
//! buffers and sent as iovecs, never copied into the connection's output buffer. `SharedBuf`
//! deliberately has no `Debug` (it carries user bytes, LC-32).

use std::sync::Arc;

#[derive(Clone)]
pub struct SharedBuf(Arc<[u8]>);

impl SharedBuf {
    pub fn new(bytes: Vec<u8>) -> Self {
        SharedBuf(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for SharedBuf {
    fn from(v: Vec<u8>) -> Self {
        SharedBuf::new(v)
    }
}

impl AsRef<[u8]> for SharedBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
