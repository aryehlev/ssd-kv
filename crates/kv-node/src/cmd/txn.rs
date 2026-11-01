//! Transactions (DESIGN §8.5): MULTI / EXEC / DISCARD (queued per connection; EXEC runs as one
//! partition request, or one XTxn in standalone mode when partitions differ; crash-atomic through
//! the intent table) and WATCH / UNWATCH (M1.4; per-partition watchers flagged dirty by executed
//! writes and FLUSH*).
