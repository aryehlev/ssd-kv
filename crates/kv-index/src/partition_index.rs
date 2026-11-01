//! Per-partition index (DESIGN §9.1, §16.6).
//!
//! `tables` is indexed by `db * (16384 / P) + slot / P` (a slot belongs to partition `p` iff
//! `slot % P == p`). Tables are created on first insert and freed when empty. Per-db key and
//! expiry counters back DBSIZE and INFO keyspace; `nonempty` lists (with a position map) give
//! RANDOMKEY a uniformly drawn non-empty table. `databases` is 16 standalone, 1 in cluster mode.

use crate::table::SlotTable;

pub struct PartitionIndex {
    partition: u16,
    partitions: u16,
    databases: u8,
    tables: Vec<Option<Box<SlotTable>>>,
    /// Per db: slot-local indexes of non-empty tables.
    nonempty: Vec<Vec<u16>>,
    keys: Vec<u64>,
    expires: Vec<u64>,
}

impl PartitionIndex {
    pub fn new(partition: u16, partitions: u16, databases: u8) -> Self {
        todo!("DESIGN §9.1: partition index")
    }

    pub fn table(&self, db: u8, slot: u16) -> Option<&SlotTable> {
        todo!("DESIGN §9.1: table lookup")
    }

    /// creates on demand
    pub fn table_mut(&mut self, db: u8, slot: u16) -> &mut SlotTable {
        todo!("DESIGN §9.1: table lookup / create")
    }

    pub fn drop_db(&mut self, db: u8) {
        todo!("DESIGN §16.5: drop a db's tables")
    }

    pub fn drop_slot(&mut self, db: u8, slot: u16) {
        todo!("DESIGN §23.13: drop a slot's table")
    }

    pub fn keys(&self, db: u8) -> u64 {
        self.keys[usize::from(db)]
    }

    pub fn expires(&self, db: u8) -> u64 {
        self.expires[usize::from(db)]
    }

    /// A uniformly drawn non-empty table of `db` (returns its slot).
    pub fn random_table(&self, db: u8, r: u64) -> Option<u16> {
        todo!("DESIGN §16.6: random non-empty table")
    }
}
