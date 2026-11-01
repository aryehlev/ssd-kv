//! SCAN and KEYS (DESIGN §16.6, LC-17, LC-18).
//!
//! The cursor is one scalar `partition:16 | slot_local:16 | fp32`. A SCAN call runs entirely on
//! the worker owning the cursor partition: page-reference walk of the slot tables (filters
//! applied inside the walk), prefetch of at most `SCAN_STEP_MAX_PAGES` pages, then key hashes
//! recomputed from the bytes read and compared with the span bounds; only key names are emitted
//! (MATCH glob and TYPE are fixed built-in filters). KEYS runs SCAN steps with COUNT 1000 over
//! all partitions strictly one at a time, at most one step in flight per call; no SCAN-family
//! request is fanned out across workers. Cluster mode walks only slots `slot_filter()` serves.
