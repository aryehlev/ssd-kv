//! Hash commands (DESIGN §8.6, M1.4 stretch): HSET, HSETNX, HGET, HMGET, HMSET, HDEL, HGETALL,
//! HKEYS, HVALS, HLEN, HEXISTS, HINCRBY, HINCRBYFLOAT, HSTRLEN. Value encoding: `u32 n_fields`
//! then `(varint flen, field, varint vlen, value)` in insertion order; whole-record
//! read-modify-write up to `hash-max-bytes`; replicated as a `Put` of the whole encoded hash.
