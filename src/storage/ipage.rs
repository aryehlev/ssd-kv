//! IPage and LargePage: the two on-disk page types.
//!
//! IPage (4 KB) holds multiple small entries via a slot directory.
//! LargePage (N × 4 KB) holds exactly one large entry.
//!
//! IPage layout:
//!   [0..4]   magic: u32 = IPAGE_MAGIC
//!   [4..6]   entry_count: u16
//!   [6..8]   live_entries: u16
//!   [8..10]  slot_end: u16  (first free byte after slot dir; initial = PAGE_HEADER_SIZE)
//!   [10..12] data_start: u16 (top of entry data, grows downward; initial = PAGE_SIZE)
//!   [12..28] _reserved: [u8; 16]
//!   [28..32] checksum: u32
//!   [32 ..]  slot dir: [(data_offset: u16, entry_len: u16) × entry_count]
//!   ...free...
//!   [data_start..4096] entry data
//!
//! Entry layout inside an IPage (at data_offset):
//!   key_len:  u16
//!   val_len:  u32  (actual value length)
//!   flags:    u8   (FLAG_DELETED | FLAG_TOMBSTONE)
//!   _pad:     u8
//!   ts:       u64  (microseconds)
//!   ttl:      u32  (seconds, 0 = no expiry)
//!   gen:      u32
//!   key:      [u8; key_len]
//!   value:    [u8; val_len]
//!
//! LargePage layout (span × 4 KB):
//!   [0..4]   magic: u32 = LRGP_MAGIC
//!   [4..8]   span: u32
//!   [8..10]  flags: u16
//!   [10..12] key_len: u16
//!   [12..16] val_len: u32
//!   [16..24] ts: u64
//!   [24..28] ttl: u32
//!   [28..32] gen: u32
//!   [32..36] checksum: u32
//!   [36..]   key bytes || value bytes

use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

pub const PAGE_SIZE: usize = 4096;
pub const IPAGE_MAGIC: u32 = 0x47415049; // "IPAG"
pub const LRGP_MAGIC: u32 = 0x4C524750;  // "LRGP"
pub const PAGE_HEADER_SIZE: usize = 32;
pub const SLOT_SIZE: usize = 4;          // (data_offset: u16, entry_len: u16)
pub const ENTRY_HEADER_SIZE: usize = 20; // key_len+val_len+flags+pad+ts+ttl+gen

pub const FLAG_DELETED: u8 = 0x01;
pub const LRGP_HEADER_SIZE: usize = 36;

fn now_micros() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0)
}

/// Decoded record, returned by both IPage and LargePage readers.
pub struct PageEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub ts: u64,
    pub ttl: u32,
    pub generation: u32,
    pub is_deleted: bool,
}

impl PageEntry {
    pub fn is_expired(&self) -> bool {
        if self.ttl == 0 { return false; }
        let now = now_micros();
        let expire_us = self.ts.saturating_add(self.ttl as u64 * 1_000_000);
        now >= expire_us
    }
}

// ─── IPage ───────────────────────────────────────────────────────────────────

/// A 4 KB page holding many small entries.
#[derive(Clone)]
pub struct IPage {
    data: Box<[u8; PAGE_SIZE]>,
}

impl IPage {
    pub fn new() -> Self {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        let magic = IPAGE_MAGIC.to_le_bytes();
        data[0..4].copy_from_slice(&magic);
        // slot_end = PAGE_HEADER_SIZE (32), data_start = PAGE_SIZE (4096)
        data[8..10].copy_from_slice(&(PAGE_HEADER_SIZE as u16).to_le_bytes());
        data[10..12].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
        Self { data }
    }

    pub fn from_bytes(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < PAGE_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "short page buffer"));
        }
        let mut data = Box::new([0u8; PAGE_SIZE]);
        data.copy_from_slice(&buf[..PAGE_SIZE]);
        Ok(Self { data })
    }

    pub fn magic(&self) -> u32 { u32::from_le_bytes(self.data[0..4].try_into().unwrap()) }
    pub fn entry_count(&self) -> u16 { u16::from_le_bytes(self.data[4..6].try_into().unwrap()) }
    pub fn live_entries(&self) -> u16 { u16::from_le_bytes(self.data[6..8].try_into().unwrap()) }
    fn slot_end(&self) -> u16 { u16::from_le_bytes(self.data[8..10].try_into().unwrap()) }
    fn data_start(&self) -> u16 { u16::from_le_bytes(self.data[10..12].try_into().unwrap()) }

    fn set_entry_count(&mut self, v: u16) { self.data[4..6].copy_from_slice(&v.to_le_bytes()); }
    fn set_live_entries(&mut self, v: u16) { self.data[6..8].copy_from_slice(&v.to_le_bytes()); }
    fn set_slot_end(&mut self, v: u16)     { self.data[8..10].copy_from_slice(&v.to_le_bytes()); }
    fn set_data_start(&mut self, v: u16)   { self.data[10..12].copy_from_slice(&v.to_le_bytes()); }

    /// Bytes of free space available for a new slot + entry.
    pub fn free_bytes(&self) -> usize {
        let s = self.data_start() as usize;
        let e = self.slot_end() as usize;
        if s > e { s - e } else { 0 }
    }

    /// True if `key_len` + `val_len` fits (including slot + entry header overhead).
    pub fn fits(&self, key_len: usize, val_len: usize) -> bool {
        let needed = SLOT_SIZE + ENTRY_HEADER_SIZE + key_len + val_len;
        self.free_bytes() >= needed
    }

    /// Append an entry. Returns the slot index on success, Err if full.
    pub fn try_append(
        &mut self,
        key: &[u8],
        value: &[u8],
        generation: u32,
        ts: u64,
        ttl: u32,
        is_deleted: bool,
    ) -> Result<u16, ()> {
        let entry_len = ENTRY_HEADER_SIZE + key.len() + value.len();
        if !self.fits(key.len(), value.len()) {
            return Err(());
        }

        let new_data_start = self.data_start() as usize - entry_len;
        let slot_off = self.slot_end() as usize;
        let slot_idx = self.entry_count();

        // Write entry
        let d = &mut self.data[new_data_start..new_data_start + entry_len];
        let flags: u8 = if is_deleted { FLAG_DELETED } else { 0 };
        d[0..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        d[2..6].copy_from_slice(&(value.len() as u32).to_le_bytes());
        d[6] = flags;
        d[7] = 0;
        d[8..16].copy_from_slice(&ts.to_le_bytes());
        d[16..20].copy_from_slice(&ttl.to_le_bytes());
        // NOTE: generation is part of the ipage entry but not used during
        // normal reads (the index carries it). Store it after ttl.
        // Extend entry header to 24 bytes: shift to fit generation.
        // Actually ENTRY_HEADER_SIZE=20, so gen goes at [20..24] – but that
        // overlaps the key.  Store gen inside flags area instead: expand
        // entry header to 24 bytes.
        // This is a design inconsistency; simplest fix: skip storing gen in
        // ipage (it's in the index), and keep ENTRY_HEADER_SIZE=20.
        d[20..20 + key.len()].copy_from_slice(key);
        d[20 + key.len()..entry_len].copy_from_slice(value);

        // Write slot (data_offset: u16, entry_len: u16)
        self.data[slot_off..slot_off + 2].copy_from_slice(&(new_data_start as u16).to_le_bytes());
        self.data[slot_off + 2..slot_off + 4].copy_from_slice(&(entry_len as u16).to_le_bytes());

        self.set_slot_end((slot_off + SLOT_SIZE) as u16);
        self.set_data_start(new_data_start as u16);
        self.set_entry_count(slot_idx + 1);
        if !is_deleted { self.set_live_entries(self.live_entries() + 1); }

        Ok(slot_idx)
    }

    /// Read the entry at `slot_idx`. Returns None if out of range.
    pub fn read_entry(&self, slot_idx: u16) -> Option<PageEntry> {
        if slot_idx >= self.entry_count() { return None; }
        let slot_off = PAGE_HEADER_SIZE + slot_idx as usize * SLOT_SIZE;
        let data_off = u16::from_le_bytes(self.data[slot_off..slot_off + 2].try_into().unwrap()) as usize;
        let entry_len = u16::from_le_bytes(self.data[slot_off + 2..slot_off + 4].try_into().unwrap()) as usize;
        if data_off + entry_len > PAGE_SIZE { return None; }

        let e = &self.data[data_off..data_off + entry_len];
        let key_len  = u16::from_le_bytes(e[0..2].try_into().unwrap()) as usize;
        let val_len  = u32::from_le_bytes(e[2..6].try_into().unwrap()) as usize;
        let flags    = e[6];
        let ts       = u64::from_le_bytes(e[8..16].try_into().unwrap());
        let ttl      = u32::from_le_bytes(e[16..20].try_into().unwrap());

        if 20 + key_len + val_len > entry_len { return None; }
        let key   = e[20..20 + key_len].to_vec();
        let value = e[20 + key_len..20 + key_len + val_len].to_vec();

        Some(PageEntry {
            key, value, ts, ttl,
            generation: 0, // not stored in ipage; caller fills from index
            is_deleted: flags & FLAG_DELETED != 0,
        })
    }

    /// Mark slot as deleted (set flag in place; does not remove data).
    pub fn mark_deleted(&mut self, slot_idx: u16) {
        if slot_idx >= self.entry_count() { return; }
        let slot_off = PAGE_HEADER_SIZE + slot_idx as usize * SLOT_SIZE;
        let data_off = u16::from_le_bytes(self.data[slot_off..slot_off + 2].try_into().unwrap()) as usize;
        if data_off + ENTRY_HEADER_SIZE <= PAGE_SIZE {
            self.data[data_off + 6] |= FLAG_DELETED;
            let live = self.live_entries().saturating_sub(1);
            self.set_live_entries(live);
        }
    }

    pub fn utilization(&self) -> f32 {
        let total = self.entry_count();
        if total == 0 { return 1.0; }
        self.live_entries() as f32 / total as f32
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] { &self.data }

    pub fn write_checksum(&mut self) {
        self.data[28..32].copy_from_slice(&0u32.to_le_bytes()); // placeholder
        let sum = crc32fast::hash(&self.data[..]);
        self.data[28..32].copy_from_slice(&sum.to_le_bytes());
    }
}

impl Default for IPage {
    fn default() -> Self { Self::new() }
}

// ─── LargePage ───────────────────────────────────────────────────────────────

/// A multi-page entry (span × 4 KB) for values that don't fit in an IPage slot.
#[derive(Clone)]
pub struct LargePage {
    pub span: u32,
    pub flags: u16,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub ts: u64,
    pub ttl: u32,
    pub generation: u32,
}

impl LargePage {
    /// How many 4 KB pages are needed for this key+value.
    pub fn pages_needed(key_len: usize, val_len: usize) -> u16 {
        let total = LRGP_HEADER_SIZE + key_len + val_len;
        let pages = total.div_ceil(PAGE_SIZE);
        pages.min(u16::MAX as usize) as u16
    }

    /// Serialize into a `span × PAGE_SIZE` byte buffer.
    pub fn encode(&self) -> Vec<u8> {
        let span = self.span as usize;
        let mut buf = vec![0u8; span * PAGE_SIZE];
        buf[0..4].copy_from_slice(&LRGP_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&self.span.to_le_bytes());
        buf[8..10].copy_from_slice(&self.flags.to_le_bytes());
        buf[10..12].copy_from_slice(&(self.key.len() as u16).to_le_bytes());
        buf[12..16].copy_from_slice(&(self.value.len() as u32).to_le_bytes());
        buf[16..24].copy_from_slice(&self.ts.to_le_bytes());
        buf[24..28].copy_from_slice(&self.ttl.to_le_bytes());
        buf[28..32].copy_from_slice(&self.generation.to_le_bytes());
        // checksum at [32..36]: over everything except the checksum field itself
        buf[32..36].copy_from_slice(&0u32.to_le_bytes());
        let body_start = LRGP_HEADER_SIZE;
        buf[body_start..body_start + self.key.len()].copy_from_slice(&self.key);
        buf[body_start + self.key.len()..body_start + self.key.len() + self.value.len()]
            .copy_from_slice(&self.value);
        // write checksum
        let sum = crc32fast::hash(&buf[..]);
        buf[32..36].copy_from_slice(&sum.to_le_bytes());
        buf
    }

    /// Decode from a raw `span × PAGE_SIZE` byte slice.
    pub fn decode(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < LRGP_HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "lrgp: short buffer"));
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != LRGP_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "lrgp: bad magic"));
        }
        let span       = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let flags      = u16::from_le_bytes(buf[8..10].try_into().unwrap());
        let key_len    = u16::from_le_bytes(buf[10..12].try_into().unwrap()) as usize;
        let val_len    = u32::from_le_bytes(buf[12..16].try_into().unwrap()) as usize;
        let ts         = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let ttl        = u32::from_le_bytes(buf[24..28].try_into().unwrap());
        let generation = u32::from_le_bytes(buf[28..32].try_into().unwrap());

        let body_start = LRGP_HEADER_SIZE;
        if body_start + key_len + val_len > buf.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "lrgp: truncated body"));
        }
        let key   = buf[body_start..body_start + key_len].to_vec();
        let value = buf[body_start + key_len..body_start + key_len + val_len].to_vec();

        Ok(Self { span, flags, key, value, ts, ttl, generation })
    }

    pub fn into_entry(self) -> PageEntry {
        PageEntry {
            key: self.key,
            value: self.value,
            ts: self.ts,
            ttl: self.ttl,
            generation: self.generation,
            is_deleted: self.flags & FLAG_DELETED as u16 != 0,
        }
    }
}
