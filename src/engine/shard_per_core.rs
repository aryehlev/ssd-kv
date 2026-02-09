//! Shard-Per-Core Architecture (ScyllaDB-inspired)
//!
//! Each CPU core owns its data exclusively:
//! - No locks on hot paths
//! - No shared state between cores
//! - Explicit message passing for cross-core communication
//! - Linear scalability with core count
//!
//! This eliminates:
//! - Lock contention
//! - Cache line bouncing
//! - Context switches

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};

use crate::engine::index_entry::hash_key;
use crate::storage::write_buffer::{DiskLocation, WriteBuffer};

/// Message types for cross-core communication.
#[derive(Debug)]
pub enum ShardMessage {
    /// GET request: key, response channel
    Get {
        key: Vec<u8>,
        respond: Sender<Option<Vec<u8>>>,
    },
    /// PUT request: key, value, ttl, response channel
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: u32,
        respond: Sender<io::Result<()>>,
    },
    /// DELETE request: key, response channel
    Delete {
        key: Vec<u8>,
        respond: Sender<bool>,
    },
    /// Shutdown signal
    Shutdown,
}

/// Statistics for a single shard.
#[derive(Debug, Default)]
pub struct ShardStats {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub deletes: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub entries: AtomicU64,
}

/// A single shard owned by one CPU core.
/// All data access is single-threaded - no locks needed!
pub struct Shard {
    /// Shard ID (0..num_cores)
    id: usize,
    /// Local index keyed by full key bytes to avoid hash collisions
    index: HashMap<Vec<u8>, ShardEntry>,
    /// Local write buffer
    write_buffer: Vec<u8>,
    /// Generation counter
    generation: u32,
    /// Statistics
    stats: ShardStats,
    /// Inline value threshold
    inline_threshold: usize,
}

/// Entry in the shard's local index.
#[derive(Clone)]
struct ShardEntry {
    /// Inline value (if small enough) OR disk location
    data: ShardData,
    generation: u32,
}

#[derive(Clone)]
enum ShardData {
    /// Value stored inline (no disk I/O needed)
    Inline(Vec<u8>),
    /// Value on disk
    OnDisk { location: DiskLocation, value_len: u32 },
}

impl Shard {
    /// Create a new shard.
    pub fn new(id: usize, inline_threshold: usize) -> Self {
        Self {
            id,
            index: HashMap::with_capacity(100_000),
            write_buffer: Vec::with_capacity(1024 * 1024), // 1MB
            generation: 0,
            stats: ShardStats::default(),
            inline_threshold,
        }
    }

    /// GET - Single-threaded, no locks!
    #[inline]
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.stats.gets.fetch_add(1, Ordering::Relaxed);

        if let Some(entry) = self.index.get(key) {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return match &entry.data {
                ShardData::Inline(value) => Some(value.clone()),
                ShardData::OnDisk { .. } => {
                    // TODO: Read from disk
                    None
                }
            };
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// PUT - Single-threaded, no locks!
    #[inline]
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, _ttl: u32) -> io::Result<()> {
        self.stats.puts.fetch_add(1, Ordering::Relaxed);

        self.generation += 1;

        let data = if value.len() <= self.inline_threshold {
            ShardData::Inline(value)
        } else {
            // TODO: Write to disk, get location
            ShardData::Inline(value) // Temporary: inline everything
        };

        let entry = ShardEntry {
            data,
            generation: self.generation,
        };

        if self.index.insert(key, entry).is_none() {
            self.stats.entries.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// DELETE - Single-threaded, no locks!
    #[inline]
    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);

        if self.index.remove(key).is_some() {
            self.stats.entries.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Process a message (called from event loop).
    pub fn process_message(&mut self, msg: ShardMessage) -> bool {
        match msg {
            ShardMessage::Get { key, respond } => {
                let result = self.get(&key);
                let _ = respond.send(result);
                true
            }
            ShardMessage::Put { key, value, ttl, respond } => {
                let result = self.put(key, value, ttl);
                let _ = respond.send(result);
                true
            }
            ShardMessage::Delete { key, respond } => {
                let result = self.delete(&key);
                let _ = respond.send(result);
                true
            }
            ShardMessage::Shutdown => false,
        }
    }

    /// Get shard ID.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Get statistics.
    pub fn stats(&self) -> &ShardStats {
        &self.stats
    }
}

/// Shard-per-core engine that distributes work across CPU cores.
pub struct ShardPerCoreEngine {
    /// Channels to send messages to each shard
    senders: Vec<Sender<ShardMessage>>,
    /// Worker threads (one per core)
    workers: Vec<JoinHandle<()>>,
    /// Number of shards
    num_shards: usize,
}

impl ShardPerCoreEngine {
    /// Create a new shard-per-core engine.
    pub fn new(num_cores: usize) -> Self {
        let mut senders = Vec::with_capacity(num_cores);
        let mut workers = Vec::with_capacity(num_cores);

        for core_id in 0..num_cores {
            let (tx, rx) = bounded::<ShardMessage>(10_000);
            senders.push(tx);

            // Spawn worker thread pinned to specific core
            let handle = thread::Builder::new()
                .name(format!("shard-{}", core_id))
                .spawn(move || {
                    // Pin to CPU core
                    let _ = crate::perf::pin_to_cpu(core_id);

                    // Create shard owned by this thread
                    let mut shard = Shard::new(core_id, 64);

                    // Event loop - process messages until shutdown
                    while let Ok(msg) = rx.recv() {
                        if !shard.process_message(msg) {
                            break;
                        }
                    }
                })
                .expect("Failed to spawn shard worker");

            workers.push(handle);
        }

        Self {
            senders,
            workers,
            num_shards: num_cores,
        }
    }

    /// Route key to appropriate shard.
    #[inline]
    fn shard_for_key(&self, key: &[u8]) -> usize {
        let hash = hash_key(key);
        (hash as usize) % self.num_shards
    }

    /// Synchronous GET.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard_id = self.shard_for_key(key);
        let (tx, rx) = bounded(1);

        let msg = ShardMessage::Get {
            key: key.to_vec(),
            respond: tx,
        };

        if self.senders[shard_id].send(msg).is_ok() {
            rx.recv().ok().flatten()
        } else {
            None
        }
    }

    /// Synchronous PUT.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>, ttl: u32) -> io::Result<()> {
        let shard_id = self.shard_for_key(&key);
        let (tx, rx) = bounded(1);

        let msg = ShardMessage::Put {
            key,
            value,
            ttl,
            respond: tx,
        };

        self.senders[shard_id]
            .send(msg)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Shard unavailable"))?;

        rx.recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "No response"))?
    }

    /// Synchronous DELETE.
    pub fn delete(&self, key: &[u8]) -> bool {
        let shard_id = self.shard_for_key(key);
        let (tx, rx) = bounded(1);

        let msg = ShardMessage::Delete {
            key: key.to_vec(),
            respond: tx,
        };

        if self.senders[shard_id].send(msg).is_ok() {
            rx.recv().unwrap_or(false)
        } else {
            false
        }
    }

    /// Fire-and-forget PUT (no response waiting).
    pub fn put_async(&self, key: Vec<u8>, value: Vec<u8>, ttl: u32) -> bool {
        let shard_id = self.shard_for_key(&key);
        let (tx, _rx) = bounded(1); // Response ignored

        let msg = ShardMessage::Put {
            key,
            value,
            ttl,
            respond: tx,
        };

        self.senders[shard_id].try_send(msg).is_ok()
    }

    /// Shutdown all shards.
    pub fn shutdown(self) {
        for sender in &self.senders {
            let _ = sender.send(ShardMessage::Shutdown);
        }

        for worker in self.workers {
            let _ = worker.join();
        }
    }

    /// Get number of shards.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }
}

/// Lock-free single-writer multi-reader value cell.
/// One thread writes, many can read without locks.
pub struct SwmrCell<T> {
    value: UnsafeCell<T>,
    version: AtomicU64,
}

unsafe impl<T: Send> Send for SwmrCell<T> {}
unsafe impl<T: Send + Sync> Sync for SwmrCell<T> {}

impl<T: Clone> SwmrCell<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: UnsafeCell::new(value),
            version: AtomicU64::new(0),
        }
    }

    /// Read the value (lock-free, wait-free).
    pub fn read(&self) -> T {
        loop {
            let v1 = self.version.load(Ordering::Acquire);

            // Read value
            let value = unsafe { (*self.value.get()).clone() };

            let v2 = self.version.load(Ordering::Acquire);

            // If version didn't change, read was consistent
            if v1 == v2 && v1 & 1 == 0 {
                return value;
            }

            // Otherwise, writer was active - retry
            std::hint::spin_loop();
        }
    }

    /// Write the value (single writer only!).
    pub fn write(&self, value: T) {
        // Mark write in progress (odd version).
        // AcqRel ensures the value write below cannot be reordered before
        // this version increment (Acquire), and prior writes are visible (Release).
        self.version.fetch_add(1, Ordering::AcqRel);

        // Write value
        unsafe {
            *self.value.get() = value;
        }

        // Mark write complete (even version).
        // Release ensures the value write above is visible before version becomes even.
        self.version.fetch_add(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_basic() {
        let mut shard = Shard::new(0, 64);

        // PUT
        shard.put(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();

        // GET
        let value = shard.get(b"key1");
        assert_eq!(value, Some(b"value1".to_vec()));

        // DELETE
        assert!(shard.delete(b"key1"));
        assert_eq!(shard.get(b"key1"), None);
    }

    #[test]
    fn test_shard_per_core_engine() {
        let engine = ShardPerCoreEngine::new(2);

        // PUT
        engine.put(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
        engine.put(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();

        // GET
        assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2"), Some(b"value2".to_vec()));

        // DELETE
        assert!(engine.delete(b"key1"));
        assert_eq!(engine.get(b"key1"), None);

        engine.shutdown();
    }

    #[test]
    fn test_swmr_cell() {
        let cell = SwmrCell::new(42u64);

        assert_eq!(cell.read(), 42);

        cell.write(100);
        assert_eq!(cell.read(), 100);
    }
}
