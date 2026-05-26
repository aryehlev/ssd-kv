//! WriteBuffer: compatibility shim for test harness.

/// No-op DiskLocation shim for test code that imports it.
#[derive(Debug, Clone, Copy, Default)]
pub struct DiskLocation;

/// No-op write buffer (replaced by eager ipage flush in SegmentManager).
pub struct WriteBuffer;

impl WriteBuffer {
    /// Both arguments are ignored; this is a compatibility stub.
    #[allow(unused_variables)]
    pub fn new(capacity: u32, max_value_size: usize) -> Self {
        Self
    }
}
