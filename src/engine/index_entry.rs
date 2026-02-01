//! Index entry: 48-byte cache-aligned structure for in-memory key lookup.

use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};

use crate::storage::write_buffer::DiskLocation;

/// Maximum inline key size (keys larger than this are heap-allocated).
pub const MAX_INLINE_KEY_SIZE: usize = 23;

/// Maximum inline value size (values larger than this go to disk).
/// Small values stored in index = no disk read needed!
pub const MAX_INLINE_VALUE_SIZE: usize = 64;

/// Key storage: either inline (≤23 bytes) or heap-allocated.
#[derive(Clone)]
pub enum KeyStorage {
    /// Key stored inline (up to 23 bytes + 1 byte for length).
    Inline {
        len: u8,
        data: [u8; MAX_INLINE_KEY_SIZE],
    },
    /// Key stored on heap.
    Heap(Box<[u8]>),
}

impl KeyStorage {
    /// Creates key storage from a byte slice.
    pub fn new(key: &[u8]) -> Self {
        if key.len() <= MAX_INLINE_KEY_SIZE {
            let mut data = [0u8; MAX_INLINE_KEY_SIZE];
            data[..key.len()].copy_from_slice(key);
            Self::Inline {
                len: key.len() as u8,
                data,
            }
        } else {
            Self::Heap(key.to_vec().into_boxed_slice())
        }
    }

    /// Returns the key as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Inline { len, data } => &data[..*len as usize],
            Self::Heap(data) => data,
        }
    }

    /// Returns the key length.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => *len as usize,
            Self::Heap(data) => data.len(),
        }
    }

    /// Returns true if the key is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if the key is stored inline.
    #[inline]
    pub fn is_inline(&self) -> bool {
        matches!(self, Self::Inline { .. })
    }
}

impl std::fmt::Debug for KeyStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyStorage({:?})", self.as_bytes())
    }
}

impl PartialEq for KeyStorage {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for KeyStorage {}

impl std::hash::Hash for KeyStorage {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

/// Entry flags.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryFlags {
    None = 0,
    Deleted = 1,
    Pending = 2, // Write in progress
}

impl From<u8> for EntryFlags {
    fn from(v: u8) -> Self {
        match v {
            1 => EntryFlags::Deleted,
            2 => EntryFlags::Pending,
            _ => EntryFlags::None,
        }
    }
}

/// Value storage: inline for small values, otherwise on disk.
#[derive(Clone)]
pub enum ValueStorage {
    /// Value stored inline (up to 64 bytes) - NO DISK READ NEEDED!
    Inline { len: u8, data: [u8; MAX_INLINE_VALUE_SIZE] },
    /// Value stored on disk at the given location.
    OnDisk,
}

impl ValueStorage {
    /// Creates inline storage if value fits, otherwise OnDisk.
    pub fn new(value: Option<&[u8]>) -> Self {
        match value {
            Some(v) if v.len() <= MAX_INLINE_VALUE_SIZE => {
                let mut data = [0u8; MAX_INLINE_VALUE_SIZE];
                data[..v.len()].copy_from_slice(v);
                Self::Inline { len: v.len() as u8, data }
            }
            _ => Self::OnDisk,
        }
    }

    /// Returns the inline value if available.
    #[inline]
    pub fn get_inline(&self) -> Option<&[u8]> {
        match self {
            Self::Inline { len, data } => Some(&data[..*len as usize]),
            Self::OnDisk => None,
        }
    }

    /// Returns true if value is stored inline.
    #[inline]
    pub fn is_inline(&self) -> bool {
        matches!(self, Self::Inline { .. })
    }
}

impl std::fmt::Debug for ValueStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inline { len, .. } => write!(f, "Inline({})", len),
            Self::OnDisk => write!(f, "OnDisk"),
        }
    }
}

/// Index entry: represents a key's metadata in the in-memory index.
#[derive(Clone)]
pub struct IndexEntry {
    /// Hash of the key for fast comparison.
    pub key_hash: u64,
    /// The key itself (inline for small keys).
    pub key: KeyStorage,
    /// Location on disk.
    pub location: DiskLocation,
    /// Generation number for conflict resolution.
    pub generation: u32,
    /// Value length in bytes.
    pub value_len: u32,
    /// Entry flags.
    pub flags: EntryFlags,
    /// Inline value storage for small values (≤64 bytes).
    pub inline_value: ValueStorage,
}

impl IndexEntry {
    /// Creates a new index entry.
    pub fn new(
        key: &[u8],
        key_hash: u64,
        location: DiskLocation,
        generation: u32,
        value_len: u32,
    ) -> Self {
        Self {
            key_hash,
            key: KeyStorage::new(key),
            location,
            generation,
            value_len,
            flags: EntryFlags::None,
            inline_value: ValueStorage::OnDisk,
        }
    }

    /// Creates a new index entry with an inline value.
    pub fn new_with_value(
        key: &[u8],
        key_hash: u64,
        location: DiskLocation,
        generation: u32,
        value: &[u8],
    ) -> Self {
        Self {
            key_hash,
            key: KeyStorage::new(key),
            location,
            generation,
            value_len: value.len() as u32,
            flags: EntryFlags::None,
            inline_value: ValueStorage::new(Some(value)),
        }
    }

    /// Creates a deleted entry (tombstone in index).
    pub fn deleted(key: &[u8], key_hash: u64, generation: u32) -> Self {
        Self {
            key_hash,
            key: KeyStorage::new(key),
            location: DiskLocation::new(0, 0, 0),
            generation,
            value_len: 0,
            flags: EntryFlags::Deleted,
            inline_value: ValueStorage::OnDisk,
        }
    }

    /// Returns the inline value if available (avoids disk read!).
    #[inline]
    pub fn get_inline_value(&self) -> Option<&[u8]> {
        self.inline_value.get_inline()
    }

    /// Returns true if value is stored inline.
    #[inline]
    pub fn has_inline_value(&self) -> bool {
        self.inline_value.is_inline()
    }

    /// Returns true if this entry matches the given key.
    #[inline]
    pub fn matches(&self, key: &[u8], key_hash: u64) -> bool {
        // Fast path: compare hashes first
        self.key_hash == key_hash && self.key.as_bytes() == key
    }

    /// Returns true if this entry is deleted.
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.flags == EntryFlags::Deleted
    }

    /// Returns true if this entry is a pending write.
    #[inline]
    pub fn is_pending(&self) -> bool {
        self.flags == EntryFlags::Pending
    }

    /// Returns true if this entry is live (not deleted).
    #[inline]
    pub fn is_live(&self) -> bool {
        self.flags == EntryFlags::None
    }

    /// Updates the entry with a new location and generation.
    pub fn update(&mut self, location: DiskLocation, generation: u32, value_len: u32) {
        self.location = location;
        self.generation = generation;
        self.value_len = value_len;
        self.flags = EntryFlags::None;
    }

    /// Marks the entry as deleted.
    pub fn mark_deleted(&mut self, generation: u32) {
        self.generation = generation;
        self.flags = EntryFlags::Deleted;
    }

    /// Returns the key hash.
    #[inline]
    pub fn hash(&self) -> u64 {
        self.key_hash
    }

    /// Returns the key bytes.
    #[inline]
    pub fn key(&self) -> &[u8] {
        self.key.as_bytes()
    }
}

impl std::fmt::Debug for IndexEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexEntry")
            .field("key_hash", &format_args!("{:016x}", self.key_hash))
            .field("key", &self.key)
            .field("location", &self.location)
            .field("generation", &self.generation)
            .field("value_len", &self.value_len)
            .field("flags", &self.flags)
            .finish()
    }
}

/// Computes the xxhash3 hash of a key.
#[inline]
pub fn hash_key(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_storage_inline() {
        let key = b"short_key";
        let storage = KeyStorage::new(key);

        assert!(storage.is_inline());
        assert_eq!(storage.as_bytes(), key);
        assert_eq!(storage.len(), key.len());
    }

    #[test]
    fn test_key_storage_heap() {
        let key = b"this_is_a_very_long_key_that_exceeds_inline_limit";
        let storage = KeyStorage::new(key);

        assert!(!storage.is_inline());
        assert_eq!(storage.as_bytes(), key);
        assert_eq!(storage.len(), key.len());
    }

    #[test]
    fn test_index_entry_matches() {
        let key = b"test_key";
        let hash = hash_key(key);
        let entry = IndexEntry::new(key, hash, DiskLocation::new(0, 0, 0), 1, 100);

        assert!(entry.matches(key, hash));
        assert!(!entry.matches(b"other_key", hash_key(b"other_key")));
    }

    #[test]
    fn test_index_entry_lifecycle() {
        let key = b"lifecycle_key";
        let hash = hash_key(key);
        let mut entry = IndexEntry::new(key, hash, DiskLocation::new(0, 0, 0), 1, 100);

        assert!(entry.is_live());

        entry.update(DiskLocation::new(1, 2, 3), 2, 200);
        assert_eq!(entry.generation, 2);
        assert_eq!(entry.value_len, 200);

        entry.mark_deleted(3);
        assert!(entry.is_deleted());
        assert_eq!(entry.generation, 3);
    }
}

#[cfg(test)]
mod hash_tests {
    use super::*;
    use crate::engine::index_entry::hash_key;

    #[test]
    fn test_hash_collision_check() {
        let key1 = b"key-with.special_chars:123";
        let key2 = b"integrity_key_00011";
        
        let hash1 = hash_key(key1);
        let hash2 = hash_key(key2);
        
        println!("Key 1: {:?} -> hash {:#018x}", String::from_utf8_lossy(key1), hash1);
        println!("Key 2: {:?} -> hash {:#018x}", String::from_utf8_lossy(key2), hash2);
        println!("Hashes equal: {}", hash1 == hash2);
        
        // Check shard (high 8 bits)
        let shard1 = ((hash1 >> 56) as usize) % 256;
        let shard2 = ((hash2 >> 56) as usize) % 256;
        println!("Shard 1: {}, Shard 2: {}", shard1, shard2);
        
        assert_ne!(hash1, hash2, "Unexpected hash collision!");
    }
}
