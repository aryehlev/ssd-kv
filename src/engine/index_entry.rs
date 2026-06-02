//! Index entry: key + SSD location stored in RAM.

pub const MAX_INLINE_KEY_SIZE: usize = 23;

/// Location of a record on SSD.
///
/// `span == 1` → normal IPage entry at `(file_id, ipage_idx, slot_idx)`.
/// `span > 1`  → LargePage spanning `span` consecutive 4 KB pages starting
///               at `(file_id, ipage_idx)`; `slot_idx` is unused.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordLocation {
    pub file_id: u32,
    pub ipage_idx: u32,
    pub slot_idx: u16,
    pub span: u16,
}

impl RecordLocation {
    #[inline]
    pub fn ipage(file_id: u32, ipage_idx: u32, slot_idx: u16) -> Self {
        Self { file_id, ipage_idx, slot_idx, span: 1 }
    }

    #[inline]
    pub fn large(file_id: u32, ipage_idx: u32, span: u16) -> Self {
        Self { file_id, ipage_idx, slot_idx: 0, span }
    }

    #[inline]
    pub fn is_large(&self) -> bool {
        self.span > 1
    }
}

/// Key storage: inline ≤23 bytes, heap otherwise.
#[derive(Clone)]
pub enum KeyStorage {
    Inline { len: u8, data: [u8; MAX_INLINE_KEY_SIZE] },
    Heap(Box<[u8]>),
}

impl KeyStorage {
    pub fn new(key: &[u8]) -> Self {
        if key.len() <= MAX_INLINE_KEY_SIZE {
            let mut data = [0u8; MAX_INLINE_KEY_SIZE];
            data[..key.len()].copy_from_slice(key);
            Self::Inline { len: key.len() as u8, data }
        } else {
            Self::Heap(key.to_vec().into_boxed_slice())
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Inline { len, data } => &data[..*len as usize],
            Self::Heap(data) => data,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => *len as usize,
            Self::Heap(data) => data.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

impl std::fmt::Debug for KeyStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyStorage({:?})", self.as_bytes())
    }
}
impl PartialEq for KeyStorage {
    fn eq(&self, other: &Self) -> bool { self.as_bytes() == other.as_bytes() }
}
impl Eq for KeyStorage {}
impl std::hash::Hash for KeyStorage {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.as_bytes().hash(state); }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryFlags {
    None = 0,
    Deleted = 1,
}
impl From<u8> for EntryFlags {
    fn from(v: u8) -> Self { if v == 1 { EntryFlags::Deleted } else { EntryFlags::None } }
}

/// In-memory index entry: key, SSD location, generation, value length.
#[derive(Clone)]
pub struct IndexEntry {
    pub key_hash: u64,
    pub key: KeyStorage,
    pub location: RecordLocation,
    pub generation: u32,
    pub value_len: u32,
    pub flags: EntryFlags,
}

impl IndexEntry {
    pub fn new(key: &[u8], key_hash: u64, location: RecordLocation, generation: u32, value_len: u32) -> Self {
        Self { key_hash, key: KeyStorage::new(key), location, generation, value_len, flags: EntryFlags::None }
    }

    pub fn deleted(key: &[u8], key_hash: u64, generation: u32) -> Self {
        Self {
            key_hash,
            key: KeyStorage::new(key),
            location: RecordLocation::default(),
            generation,
            value_len: 0,
            flags: EntryFlags::Deleted,
        }
    }

    #[inline]
    pub fn matches(&self, key: &[u8], key_hash: u64) -> bool {
        if self.key_hash != key_hash { return false; }
        crate::perf::simd::simd_key_eq(self.key.as_bytes(), key)
    }

    #[inline]
    pub fn is_deleted(&self) -> bool { self.flags == EntryFlags::Deleted }
    #[inline]
    pub fn is_live(&self) -> bool { self.flags == EntryFlags::None }

    pub fn update(&mut self, location: RecordLocation, generation: u32, value_len: u32) {
        self.location = location;
        self.generation = generation;
        self.value_len = value_len;
        self.flags = EntryFlags::None;
    }

    pub fn mark_deleted(&mut self, generation: u32) {
        self.generation = generation;
        self.flags = EntryFlags::Deleted;
    }

    #[inline]
    pub fn key(&self) -> &[u8] { self.key.as_bytes() }
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

#[inline]
pub fn hash_key(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}
