//! Intent table: crash-atomic multi-key commits (DESIGN §10.7, §14.6, §17.5).
//!
//! Header (AbBlock magic `KVINTNT1`): `txn_floor` (ids handed out are below it; reserved
//! 2^20 at a time) and `resolved_w` (every txn id <= W is resolved). 64 slots, each with two
//! 512-byte copies in separate `lba` sectors: `"KVIS"`, crc32c, `copy_seq`, `n <= 60` committed
//! txn ids. The writer overwrites the copy with the lower `copy_seq`. A commit batch goes into
//! the lowest-index slot whose open ids plus the batch fit in 60; resolved ids are dropped by
//! the next rewrite of a slot after a durable `resolved_w` covers them (E10). Slots are never
//! selected by rotation and nothing is ordered by slot index or `copy_seq` across slots. The
//! table holds only committed txn ids: no values, keys or record headers (LC-02).
//!
//! A TXN-flagged record with id `t` is valid iff `t <= resolved_w` or `t` is listed in some
//! slot's newest valid copy.
//!
//! Txn credits: 3840 commit positions divided statically among workers (`3840 / W` each); a
//! worker takes one before it consumes a multi-key write and gets it back when the txn resolves,
//! so a consumed txn never waits for a slot (LC-15).

use core::sync::atomic::AtomicU64;

use smallvec::SmallVec;

/// `KVINTNT1`
pub const INTENT_MAGIC: u64 = kv_common::abblock::magic(b"KVINTNT1");
/// `"KVIS"` slot copy magic.
pub const INTENT_SLOT_MAGIC: u32 = u32::from_le_bytes(*b"KVIS");
/// Bytes of one slot copy (the first 512 B of its sector).
pub const INTENT_SLOT_COPY_BYTES: usize = 512;

pub const INTENT_SLOTS: usize = 64;
/// 3840 txn credits
pub const IDS_PER_COPY: usize = 60;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct IntentHeader {
    pub txn_floor: u64,
    pub resolved_w: u64,
}

impl IntentHeader {
    pub fn encode(&self, seq: u64, out: &mut [u8; crate::layout::PAGE]) {
        todo!("DESIGN §10.7: intent header encode")
    }

    pub fn decode(b: &[u8]) -> Option<(u64, Self)> {
        todo!("DESIGN §10.7: intent header decode")
    }
}

/// One copy of one intent slot.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct IntentSlot {
    pub copy_seq: u64,
    pub txn_ids: SmallVec<[u64; 60]>,
}

impl IntentSlot {
    pub fn encode(&self, out: &mut [u8; INTENT_SLOT_COPY_BYTES]) {
        todo!("DESIGN §10.7: slot copy encode")
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        todo!("DESIGN §10.7: slot copy decode")
    }
}

/// Per-worker pool of txn credits: `3840 / W` each; taken before consuming a multi-key write,
/// returned on `MetaDone::Resolved`.
pub struct TxnCredits {
    total: u32,
    in_use: u32,
}

impl TxnCredits {
    pub fn new(total: u32) -> Self {
        TxnCredits { total, in_use: 0 }
    }

    pub fn try_take(&mut self) -> bool {
        if self.in_use < self.total {
            self.in_use += 1;
            true
        } else {
            false
        }
    }

    pub fn give_back(&mut self, n: u32) {
        debug_assert!(n <= self.in_use);
        self.in_use -= n;
    }

    pub fn in_use(&self) -> u32 {
        self.in_use
    }
}

/// Node-global txn id source inside the reserved `txn_floor` range; the meta thread extends the
/// floor by 2^20 at half use, so `next` never waits.
pub struct TxnIds {
    next: AtomicU64,
    floor: AtomicU64,
}

impl TxnIds {
    pub fn next(&self) -> u64 {
        todo!("DESIGN §14.6: txn id allocation")
    }
}
