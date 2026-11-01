//! INFO (DESIGN §21): sections server, clients, memory, persistence, stats, replication (only
//! `role`, `connected_slaves`, `master_link_status`: no shard id, term, seq, lag position or peer
//! identity, LC-24, LC-28), keyspace, cluster, commandstats, latencystats and kvd. Bulk reply
//! (RESP3: verbatim `txt`).
