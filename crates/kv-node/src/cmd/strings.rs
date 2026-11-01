//! String commands (DESIGN §8.3, Redis 7.0.15 semantics): GET, SET (NX/XX/GET/EX/PX/EXAT/PXAT/
//! KEEPTTL), SETNX, SETEX, PSETEX, GETSET, GETDEL, GETEX, MGET, MSET, MSETNX, APPEND, STRLEN,
//! GETRANGE/SUBSTR, SETRANGE, INCR/DECR/INCRBY/DECRBY (`string2ll`, overflow errors) and
//! INCRBYFLOAT (long double through `kv_common::ldouble`, stored and replicated as the formatted
//! result).
