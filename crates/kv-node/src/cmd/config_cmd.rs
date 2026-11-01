//! CONFIG GET / SET / RESETSTAT (DESIGN §20.2). CONFIG GET also answers the compatibility
//! keys clients query (`save ""`, `appendonly no`, `maxmemory 0`, `maxmemory-policy
//! noeviction`, `cluster-enabled`, `databases`); unknown patterns give an empty array. CONFIG
//! REWRITE replies `-ERR CONFIG REWRITE not supported`.
