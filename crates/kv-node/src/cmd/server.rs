//! Server commands (DESIGN §8.2): COMMAND (from the kv-cmd table), DBSIZE (sum of the
//! per-partition counters; counts logically expired but unreclaimed keys like Redis), FLUSHDB /
//! FLUSHALL (per-partition seq cutoffs, reply after the cutoff metadata is durable, §16.5), TIME,
//! SAVE / BGSAVE / LASTSAVE (durability barriers; no dump file exists), SHUTDOWN (drain, snapshot
//! when it fits, CLEAN), WAIT / WAITAOF, TOUCH, and the REPLICAOF/SLAVEOF refusal
//! `-ERR replication is managed by kvadm`.
