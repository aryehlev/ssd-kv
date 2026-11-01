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

impl From<Arc<[u8]>> for SharedBuf {
    fn from(a: Arc<[u8]>) -> Self {
        SharedBuf(a)
    }
}

impl AsRef<[u8]> for SharedBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl core::ops::Deref for SharedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::SharedBuf;

    #[test]
    fn shares_without_copying() {
        let a = SharedBuf::new(vec![1, 2, 3]);
        let b = a.clone();
        assert_eq!(a.as_bytes().as_ptr(), b.as_bytes().as_ptr());
        assert_eq!(&*b, &[1, 2, 3]);
        assert_eq!(b.len(), 3);
        assert!(!b.is_empty());
        assert!(SharedBuf::from(Vec::new()).is_empty());
    }
}
