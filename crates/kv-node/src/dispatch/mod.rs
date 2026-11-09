//! Command dispatch (DESIGN §6.2, §6.5, §6.8, §23.6).
//!
//! * [`router`]: command lookup, arity, auth, MULTI queueing, the cluster routing gate and
//!   `slot -> partition -> worker` routing (local submit or `XMsg::Cmd`).
//! * [`multikey`]: standalone fan-out of MGET/EXISTS/DEL/UNLINK/TOUCH across partitions
//!   (exactly one sub-request per distinct partition, LC-37) and the gather of their replies.
//! * [`xtxn`]: standalone cross-partition transactions (MSET, MSETNX, RENAME, RENAMENX, COPY,
//!   EXEC spanning partitions) under the node-global conflict gate.
//! * [`scan`]: SCAN / KEYS / RANDOMKEY steps, one partition at a time on its owner (LC-18).
//!
//! Dispatch never parks a parsed command because of load and uses no clocks (LC-14).

pub mod multikey;
pub mod router;
pub mod scan;
pub mod xtxn;

use kv_storage::keyspace::StoreError;

/// The reply text of a storage error (§11.6, §15, §18.1, §22): the same texts as
/// `cmd::write_store_error`, for replies the runtime produces itself (transactions, scans).
pub fn store_error_text(e: StoreError) -> &'static [u8] {
    match e {
        StoreError::WrongType => b"WRONGTYPE Operation against a key holding the wrong kind of value",
        StoreError::OutOfSpace => b"OOM storage full: command not allowed when device fill > 'max-storage-fill'",
        StoreError::TooLarge => b"ERR string exceeds maximum allowed size (proto-max-bulk-len)",
        StoreError::Corrupt(_) => b"ERR storage corruption detected",
        StoreError::Io(_) | StoreError::ReadOnly => b"ERR storage I/O error",
        StoreError::Loading => router::LOADING,
        StoreError::NeedMore => b"ERR storage page not loaded",
    }
}

/// Appends the error reply of a storage error.
pub fn write_store_error(e: StoreError, w: &mut kv_resp::ReplyWriter<'_>) {
    w.error(store_error_text(e));
}
