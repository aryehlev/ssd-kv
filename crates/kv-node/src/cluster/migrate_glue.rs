//! Migration endpoints of the node's partitions (DESIGN §23.13).
//!
//! Source: the partition streams its migrating slots through the page-reference walk, paced by
//! static rates, forwards live groups of keys below the walk position, records its position
//! barriers and reports `MigrationReady` / seals after cutover. Destination: each partition
//! holding migrating slots dials the source with `Hello { migration }`, applies the copy as its
//! own writes and answers TRYAGAIN for those slots until the seal is committed. A partial copy
//! after a failure stays inert (not served, excluded by `slot_filter()`) until the operator
//! aborts or resumes.

use kv_repl::migrate::{MigrationDest, MigrationSource};

/// One migration endpoint of one partition.
pub enum MigrationEndpoint {
    Source(MigrationSource),
    Dest(MigrationDest),
}
