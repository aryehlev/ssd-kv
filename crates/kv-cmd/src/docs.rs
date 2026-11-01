//! COMMAND, COMMAND INFO, COMMAND DOCS, COMMAND LIST and COMMAND COUNT replies (DESIGN §8.2).
//!
//! COMMAND INFO uses the Redis 7 reply shape (name, arity, flags, first key, last key, step,
//! ACL categories, tips, key specs, subcommands) with Redis 7.0.15's data for every command in
//! the table (Redis 7.2's for the four commands the spec adds from it), including RESP3 types
//! (flags, categories, tips, key specs and subcommands are sets; key specs are maps). COMMAND
//! DOCS returns only `summary`, `since` and `group` for each command (plus `subcommands` for
//! container commands, a map of their docs in the same shape). Redis returns command lists in
//! dictionary order; kvssd returns table order. The metadata is Redis's (BSD-3-Clause, see
//! NOTICE).

use kv_common::glob::glob_match;
use kv_resp::ReplyWriter;

use crate::spec::{CmdId, CommandSpec, KeySpec};
use crate::table::{all, all_with_subcommands, lookup_fullname};

/// COMMAND INFO metadata of one command, as Redis 7 reports it.
struct Meta {
    /// Command flags (`write`, `readonly`, `fast`, ...).
    flags: &'static [&'static str],
    /// ACL categories (`@read`, `@string`, ...).
    acl: &'static [&'static str],
    /// Command tips (`nondeterministic_output`, `request_policy:all_shards`, ...).
    tips: &'static [&'static str],
    key_specs: &'static [KeySpecDoc],
}

/// One Redis 7 key specification with an `index` begin-search and a `range` find-keys step
/// (the only kinds the commands of the table use).
struct KeySpecDoc {
    notes: Option<&'static str>,
    flags: &'static [&'static str],
    index: i64,
    lastkey: i64,
    keystep: i64,
    limit: i64,
}

fn meta(id: CmdId) -> &'static Meta {
    match id {
        CmdId::Ping => &Meta {
            flags: &["fast"],
            acl: &["@fast", "@connection"],
            tips: &["request_policy:all_shards", "response_policy:all_succeeded"],
            key_specs: &[],
        },
        CmdId::Echo => &Meta {
            flags: &["loading", "stale", "fast"],
            acl: &["@fast", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Hello => &Meta {
            flags: &[
                "noscript",
                "loading",
                "stale",
                "fast",
                "no_auth",
                "allow_busy",
            ],
            acl: &["@fast", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Auth => &Meta {
            flags: &[
                "noscript",
                "loading",
                "stale",
                "fast",
                "no_auth",
                "allow_busy",
            ],
            acl: &["@fast", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Select => &Meta {
            flags: &["loading", "stale", "fast"],
            acl: &["@fast", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Quit => &Meta {
            flags: &[
                "noscript",
                "loading",
                "stale",
                "fast",
                "no_auth",
                "allow_busy",
            ],
            acl: &["@fast", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Reset => &Meta {
            flags: &[
                "noscript",
                "loading",
                "stale",
                "fast",
                "no_auth",
                "allow_busy",
            ],
            acl: &["@fast", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Client => &Meta {
            flags: &[],
            acl: &["@slow"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClientId => &Meta {
            flags: &["noscript", "loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClientGetname => &Meta {
            flags: &["noscript", "loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClientSetname => &Meta {
            flags: &["noscript", "loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClientInfo => &Meta {
            flags: &["noscript", "loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClientList => &Meta {
            flags: &["admin", "noscript", "loading", "stale"],
            acl: &["@admin", "@slow", "@dangerous", "@connection"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClientSetinfo => &Meta {
            flags: &["noscript", "loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &["request_policy:all_nodes", "response_policy:all_succeeded"],
            key_specs: &[],
        },
        CmdId::ClientNoEvict => &Meta {
            flags: &["admin", "noscript", "loading", "stale"],
            acl: &["@admin", "@slow", "@dangerous", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClientNoTouch => &Meta {
            flags: &["noscript", "loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClientKill => &Meta {
            flags: &["admin", "noscript", "loading", "stale"],
            acl: &["@admin", "@slow", "@dangerous", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClientReply => &Meta {
            flags: &["noscript", "loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Command => &Meta {
            flags: &["loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &["nondeterministic_output_order"],
            key_specs: &[],
        },
        CmdId::CommandCount => &Meta {
            flags: &["loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::CommandInfo => &Meta {
            flags: &["loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &["nondeterministic_output_order"],
            key_specs: &[],
        },
        CmdId::CommandDocs => &Meta {
            flags: &["loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &["nondeterministic_output_order"],
            key_specs: &[],
        },
        CmdId::CommandList => &Meta {
            flags: &["loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &["nondeterministic_output_order"],
            key_specs: &[],
        },
        CmdId::CommandGetkeys => &Meta {
            flags: &["loading", "stale"],
            acl: &["@slow", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Info => &Meta {
            flags: &["loading", "stale"],
            acl: &["@slow", "@dangerous"],
            tips: &[
                "nondeterministic_output",
                "request_policy:all_shards",
                "response_policy:special",
            ],
            key_specs: &[],
        },
        CmdId::Config => &Meta {
            flags: &[],
            acl: &["@slow"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ConfigGet => &Meta {
            flags: &["admin", "noscript", "loading", "stale"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ConfigSet => &Meta {
            flags: &["admin", "noscript", "loading", "stale"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &["request_policy:all_nodes", "response_policy:all_succeeded"],
            key_specs: &[],
        },
        CmdId::ConfigResetstat => &Meta {
            flags: &["admin", "noscript", "loading", "stale"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ConfigRewrite => &Meta {
            flags: &["admin", "noscript", "loading", "stale"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Dbsize => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@keyspace", "@read", "@fast"],
            tips: &["request_policy:all_shards", "response_policy:agg_sum"],
            key_specs: &[],
        },
        CmdId::Flushdb => &Meta {
            flags: &["write"],
            acl: &["@keyspace", "@write", "@slow", "@dangerous"],
            tips: &["request_policy:all_shards", "response_policy:all_succeeded"],
            key_specs: &[],
        },
        CmdId::Flushall => &Meta {
            flags: &["write"],
            acl: &["@keyspace", "@write", "@slow", "@dangerous"],
            tips: &["request_policy:all_shards", "response_policy:all_succeeded"],
            key_specs: &[],
        },
        CmdId::Time => &Meta {
            flags: &["loading", "stale", "fast"],
            acl: &["@fast"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::Save => &Meta {
            flags: &["admin", "noscript", "no_async_loading", "no_multi"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Bgsave => &Meta {
            flags: &["admin", "noscript", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Lastsave => &Meta {
            flags: &["loading", "stale", "fast"],
            acl: &["@admin", "@fast", "@dangerous"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::Shutdown => &Meta {
            flags: &[
                "admin",
                "noscript",
                "loading",
                "stale",
                "no_multi",
                "allow_busy",
            ],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Wait => &Meta {
            flags: &["noscript"],
            acl: &["@slow", "@connection"],
            tips: &["request_policy:all_shards", "response_policy:agg_min"],
            key_specs: &[],
        },
        CmdId::Waitaof => &Meta {
            flags: &["noscript"],
            acl: &["@slow", "@connection"],
            tips: &["request_policy:all_shards", "response_policy:agg_min"],
            key_specs: &[],
        },
        CmdId::Touch => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@keyspace", "@read", "@fast"],
            tips: &["request_policy:multi_shard", "response_policy:agg_sum"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO"],
                index: 1,
                lastkey: -1,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Replicaof => &Meta {
            flags: &["admin", "noscript", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Slaveof => &Meta {
            flags: &["admin", "noscript", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Get => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@read", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Set => &Meta {
            flags: &["write", "denyoom"],
            acl: &["@write", "@string", "@slow"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: Some("RW and ACCESS due to the optional `GET` argument"),
                flags: &["RW", "access", "update", "variable_flags"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Setnx => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["OW", "insert"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Setex => &Meta {
            flags: &["write", "denyoom"],
            acl: &["@write", "@string", "@slow"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["OW", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Psetex => &Meta {
            flags: &["write", "denyoom"],
            acl: &["@write", "@string", "@slow"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["OW", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Getset => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "access", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Getdel => &Meta {
            flags: &["write", "fast"],
            acl: &["@write", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "access", "delete"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Getex => &Meta {
            flags: &["write", "fast"],
            acl: &["@write", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: Some("RW and UPDATE because it changes the TTL"),
                flags: &["RW", "access", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Mget => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@read", "@string", "@fast"],
            tips: &["request_policy:multi_shard"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: -1,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Mset => &Meta {
            flags: &["write", "denyoom"],
            acl: &["@write", "@string", "@slow"],
            tips: &[
                "request_policy:multi_shard",
                "response_policy:all_succeeded",
            ],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["OW", "update"],
                index: 1,
                lastkey: -1,
                keystep: 2,
                limit: 0,
            }],
        },
        CmdId::Msetnx => &Meta {
            flags: &["write", "denyoom"],
            acl: &["@write", "@string", "@slow"],
            tips: &["request_policy:multi_shard", "response_policy:agg_min"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["OW", "insert"],
                index: 1,
                lastkey: -1,
                keystep: 2,
                limit: 0,
            }],
        },
        CmdId::Append => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "insert"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Strlen => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@read", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Getrange => &Meta {
            flags: &["readonly"],
            acl: &["@read", "@string", "@slow"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Substr => &Meta {
            flags: &["readonly"],
            acl: &["@read", "@string", "@slow"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Setrange => &Meta {
            flags: &["write", "denyoom"],
            acl: &["@write", "@string", "@slow"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Incr => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "access", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Decr => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "access", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Incrby => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "access", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Decrby => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "access", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Incrbyfloat => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@string", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "access", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Del => &Meta {
            flags: &["write"],
            acl: &["@keyspace", "@write", "@slow"],
            tips: &["request_policy:multi_shard", "response_policy:agg_sum"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RM", "delete"],
                index: 1,
                lastkey: -1,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Unlink => &Meta {
            flags: &["write", "fast"],
            acl: &["@keyspace", "@write", "@fast"],
            tips: &["request_policy:multi_shard", "response_policy:agg_sum"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RM", "delete"],
                index: 1,
                lastkey: -1,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Exists => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@keyspace", "@read", "@fast"],
            tips: &["request_policy:multi_shard", "response_policy:agg_sum"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO"],
                index: 1,
                lastkey: -1,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Type => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@keyspace", "@read", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Rename => &Meta {
            flags: &["write"],
            acl: &["@keyspace", "@write", "@slow"],
            tips: &[],
            key_specs: &[
                KeySpecDoc {
                    notes: None,
                    flags: &["RW", "access", "delete"],
                    index: 1,
                    lastkey: 0,
                    keystep: 1,
                    limit: 0,
                },
                KeySpecDoc {
                    notes: None,
                    flags: &["OW", "update"],
                    index: 2,
                    lastkey: 0,
                    keystep: 1,
                    limit: 0,
                },
            ],
        },
        CmdId::Renamenx => &Meta {
            flags: &["write", "fast"],
            acl: &["@keyspace", "@write", "@fast"],
            tips: &[],
            key_specs: &[
                KeySpecDoc {
                    notes: None,
                    flags: &["RW", "access", "delete"],
                    index: 1,
                    lastkey: 0,
                    keystep: 1,
                    limit: 0,
                },
                KeySpecDoc {
                    notes: None,
                    flags: &["OW", "insert"],
                    index: 2,
                    lastkey: 0,
                    keystep: 1,
                    limit: 0,
                },
            ],
        },
        CmdId::Copy => &Meta {
            flags: &["write", "denyoom"],
            acl: &["@keyspace", "@write", "@slow"],
            tips: &[],
            key_specs: &[
                KeySpecDoc {
                    notes: None,
                    flags: &["RO", "access"],
                    index: 1,
                    lastkey: 0,
                    keystep: 1,
                    limit: 0,
                },
                KeySpecDoc {
                    notes: None,
                    flags: &["OW", "update"],
                    index: 2,
                    lastkey: 0,
                    keystep: 1,
                    limit: 0,
                },
            ],
        },
        CmdId::Randomkey => &Meta {
            flags: &["readonly"],
            acl: &["@keyspace", "@read", "@slow"],
            tips: &["request_policy:all_shards", "nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::Keys => &Meta {
            flags: &["readonly"],
            acl: &["@keyspace", "@read", "@slow", "@dangerous"],
            tips: &["request_policy:all_shards", "nondeterministic_output_order"],
            key_specs: &[],
        },
        CmdId::Scan => &Meta {
            flags: &["readonly"],
            acl: &["@keyspace", "@read", "@slow"],
            tips: &["nondeterministic_output", "request_policy:special"],
            key_specs: &[],
        },
        CmdId::Expire => &Meta {
            flags: &["write", "fast"],
            acl: &["@keyspace", "@write", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Pexpire => &Meta {
            flags: &["write", "fast"],
            acl: &["@keyspace", "@write", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Expireat => &Meta {
            flags: &["write", "fast"],
            acl: &["@keyspace", "@write", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Pexpireat => &Meta {
            flags: &["write", "fast"],
            acl: &["@keyspace", "@write", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Ttl => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@keyspace", "@read", "@fast"],
            tips: &["nondeterministic_output"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Pttl => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@keyspace", "@read", "@fast"],
            tips: &["nondeterministic_output"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Expiretime => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@keyspace", "@read", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Pexpiretime => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@keyspace", "@read", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Persist => &Meta {
            flags: &["write", "fast"],
            acl: &["@keyspace", "@write", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Multi => &Meta {
            flags: &["noscript", "loading", "stale", "fast", "allow_busy"],
            acl: &["@fast", "@transaction"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Exec => &Meta {
            flags: &["noscript", "loading", "stale", "skip_slowlog"],
            acl: &["@slow", "@transaction"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Discard => &Meta {
            flags: &["noscript", "loading", "stale", "fast", "allow_busy"],
            acl: &["@fast", "@transaction"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Watch => &Meta {
            flags: &["noscript", "loading", "stale", "fast", "allow_busy"],
            acl: &["@fast", "@transaction"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO"],
                index: 1,
                lastkey: -1,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Unwatch => &Meta {
            flags: &["noscript", "loading", "stale", "fast", "allow_busy"],
            acl: &["@fast", "@transaction"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Hset => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hsetnx => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "insert"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hget => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@read", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hmget => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@read", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hmset => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hdel => &Meta {
            flags: &["write", "fast"],
            acl: &["@write", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "delete"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hgetall => &Meta {
            flags: &["readonly"],
            acl: &["@read", "@hash", "@slow"],
            tips: &["nondeterministic_output_order"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hkeys => &Meta {
            flags: &["readonly"],
            acl: &["@read", "@hash", "@slow"],
            tips: &["nondeterministic_output_order"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hvals => &Meta {
            flags: &["readonly"],
            acl: &["@read", "@hash", "@slow"],
            tips: &["nondeterministic_output_order"],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO", "access"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hlen => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@read", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hexists => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@read", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hincrby => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "access", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hincrbyfloat => &Meta {
            flags: &["write", "denyoom", "fast"],
            acl: &["@write", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RW", "access", "update"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Hstrlen => &Meta {
            flags: &["readonly", "fast"],
            acl: &["@read", "@hash", "@fast"],
            tips: &[],
            key_specs: &[KeySpecDoc {
                notes: None,
                flags: &["RO"],
                index: 1,
                lastkey: 0,
                keystep: 1,
                limit: 0,
            }],
        },
        CmdId::Cluster => &Meta {
            flags: &[],
            acl: &["@slow"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterKeyslot => &Meta {
            flags: &["stale"],
            acl: &["@slow"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterMyid => &Meta {
            flags: &["stale"],
            acl: &["@slow"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterInfo => &Meta {
            flags: &["stale"],
            acl: &["@slow"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClusterSlots => &Meta {
            flags: &["stale"],
            acl: &["@slow"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClusterShards => &Meta {
            flags: &["stale"],
            acl: &["@slow"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClusterNodes => &Meta {
            flags: &["stale"],
            acl: &["@slow"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClusterReplicas => &Meta {
            flags: &["admin", "stale"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClusterSlaves => &Meta {
            flags: &["admin", "stale"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClusterLinks => &Meta {
            flags: &["stale"],
            acl: &["@slow"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClusterMyshardid => &Meta {
            flags: &["stale"],
            acl: &["@slow"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClusterCountkeysinslot => &Meta {
            flags: &["stale"],
            acl: &["@slow"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterGetkeysinslot => &Meta {
            flags: &["stale"],
            acl: &["@slow"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClusterMeet => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterAddslots => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterAddslotsrange => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterDelslots => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterDelslotsrange => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterSetslot => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterFailover => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterForget => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterReplicate => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterReset => &Meta {
            flags: &["admin", "noscript", "stale"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterFlushslots => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterBumpepoch => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::ClusterSaveconfig => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterSetConfigEpoch => &Meta {
            flags: &["admin", "stale", "no_async_loading"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::ClusterCountFailureReports => &Meta {
            flags: &["admin", "stale"],
            acl: &["@admin", "@slow", "@dangerous"],
            tips: &["nondeterministic_output"],
            key_specs: &[],
        },
        CmdId::Readonly => &Meta {
            flags: &["loading", "stale", "fast"],
            acl: &["@fast", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Readwrite => &Meta {
            flags: &["loading", "stale", "fast"],
            acl: &["@fast", "@connection"],
            tips: &[],
            key_specs: &[],
        },
        CmdId::Asking => &Meta {
            flags: &["fast"],
            acl: &["@fast", "@connection"],
            tips: &[],
            key_specs: &[],
        },
    }
}

/// Writes the COMMAND INFO entry of `spec` (a 10-element array; container commands list their
/// subcommands' entries in the last element).
pub fn write_command_info(spec: &CommandSpec, w: &mut ReplyWriter<'_>) {
    let m = meta(spec.id);
    let (first, last, step) = match spec.keys {
        KeySpec::Range { first, last, step } => {
            (i64::from(first), i64::from(last), i64::from(step))
        }
        KeySpec::None | KeySpec::Custom(_) => (0, 0, 0),
    };
    w.array(10);
    w.bulk(spec.name.as_bytes());
    w.int(i64::from(spec.arity));
    w.set(m.flags.len());
    m.flags.iter().for_each(|f| w.simple(f.as_bytes()));
    w.int(first);
    w.int(last);
    w.int(step);
    w.set(m.acl.len());
    m.acl.iter().for_each(|c| w.simple(c.as_bytes()));
    w.set(m.tips.len());
    m.tips.iter().for_each(|t| w.bulk(t.as_bytes()));
    w.set(m.key_specs.len());
    for ks in m.key_specs {
        w.map(3 + usize::from(ks.notes.is_some()));
        if let Some(notes) = ks.notes {
            w.bulk(b"notes");
            w.bulk(notes.as_bytes());
        }
        w.bulk(b"flags");
        w.set(ks.flags.len());
        ks.flags.iter().for_each(|f| w.simple(f.as_bytes()));
        w.bulk(b"begin_search");
        w.map(2);
        w.bulk(b"type");
        w.bulk(b"index");
        w.bulk(b"spec");
        w.map(1);
        w.bulk(b"index");
        w.int(ks.index);
        w.bulk(b"find_keys");
        w.map(2);
        w.bulk(b"type");
        w.bulk(b"range");
        w.bulk(b"spec");
        w.map(3);
        w.bulk(b"lastkey");
        w.int(ks.lastkey);
        w.bulk(b"keystep");
        w.int(ks.keystep);
        w.bulk(b"limit");
        w.int(ks.limit);
    }
    w.set(spec.subcommands.len());
    spec.subcommands
        .iter()
        .for_each(|s| write_command_info(s, w));
}

/// Writes the COMMAND DOCS map of `spec` (the value; the caller writes the name key):
/// `summary`, `since`, `group`, and for containers `subcommands` (name -> docs).
pub fn write_command_docs(spec: &CommandSpec, w: &mut ReplyWriter<'_>) {
    w.map(3 + usize::from(spec.is_container()));
    w.bulk(b"summary");
    w.bulk(spec.summary.as_bytes());
    w.bulk(b"since");
    w.bulk(spec.since.as_bytes());
    w.bulk(b"group");
    w.bulk(spec.group.as_bytes());
    if spec.is_container() {
        w.bulk(b"subcommands");
        w.map(spec.subcommands.len());
        for s in spec.subcommands {
            w.bulk(s.name.as_bytes());
            write_command_docs(s, w);
        }
    }
}

/// COMMAND COUNT: the number of top-level commands.
pub fn command_count() -> usize {
    all().len()
}

/// COMMAND (no arguments): the COMMAND INFO entry of every top-level command.
pub fn write_command_all(w: &mut ReplyWriter<'_>) {
    w.array(all().len());
    all().iter().for_each(|c| write_command_info(c, w));
}

/// COMMAND INFO [name ...]: one entry per name (null for an unknown one); every command when no
/// name is given. Names may use the `container|subcommand` form.
pub fn write_command_info_reply(names: &[&[u8]], w: &mut ReplyWriter<'_>) {
    if names.is_empty() {
        return write_command_all(w);
    }
    w.array(names.len());
    for n in names {
        match lookup_fullname(n) {
            Some(spec) => write_command_info(spec, w),
            None => w.null(),
        }
    }
}

/// COMMAND DOCS [name ...]: a map of full name -> docs for the names found (unknown names are
/// skipped); every top-level command when no name is given.
pub fn write_command_docs_reply(names: &[&[u8]], w: &mut ReplyWriter<'_>) {
    if names.is_empty() {
        w.map(all().len());
        for c in all() {
            w.bulk(c.name.as_bytes());
            write_command_docs(c, w);
        }
        return;
    }
    let found: smallvec::SmallVec<[&'static CommandSpec; 8]> =
        names.iter().filter_map(|n| lookup_fullname(n)).collect();
    w.map(found.len());
    for spec in found {
        w.bulk(spec.name.as_bytes());
        write_command_docs(spec, w);
    }
}

/// COMMAND LIST [FILTERBY MODULE name | ACLCAT category | PATTERN pattern]: full names of every
/// command and subcommand (no module exists, so a MODULE filter is empty; ACLCAT takes a
/// category name without `@`, unknown ones match nothing; PATTERN is a case-insensitive glob).
/// `args` are the arguments after `LIST`; anything but nothing or one FILTERBY clause is
/// `-ERR syntax error`.
pub fn write_command_list_reply(args: &[&[u8]], w: &mut ReplyWriter<'_>) {
    enum Filter<'a> {
        All,
        Module,
        AclCat(&'a [u8]),
        Pattern(&'a [u8]),
    }
    let filter = match args {
        [] => Filter::All,
        [by, kind, arg] if by.eq_ignore_ascii_case(b"filterby") => {
            if kind.eq_ignore_ascii_case(b"module") {
                Filter::Module
            } else if kind.eq_ignore_ascii_case(b"aclcat") {
                Filter::AclCat(arg)
            } else if kind.eq_ignore_ascii_case(b"pattern") {
                Filter::Pattern(arg)
            } else {
                return w.error(b"ERR syntax error");
            }
        }
        _ => return w.error(b"ERR syntax error"),
    };
    let keep = |spec: &CommandSpec| match filter {
        Filter::All => true,
        Filter::Module => false,
        Filter::AclCat(cat) => meta(spec.id)
            .acl
            .iter()
            .any(|c| c.as_bytes()[1..].eq_ignore_ascii_case(cat)),
        Filter::Pattern(p) => glob_match(p, spec.name.as_bytes(), true),
    };
    let n = all_with_subcommands().filter(|s| keep(s)).count();
    w.array(n);
    all_with_subcommands()
        .filter(|s| keep(s))
        .for_each(|s| w.bulk(s.name.as_bytes()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_resp::Proto;

    fn info(name: &str, proto: Proto) -> Vec<u8> {
        let mut b = Vec::new();
        write_command_info_reply(&[name.as_bytes()], &mut ReplyWriter::new(&mut b, proto));
        b
    }

    fn run(proto: Proto, f: impl FnOnce(&mut ReplyWriter<'_>)) -> kv_resp::Value {
        let mut b = Vec::new();
        f(&mut ReplyWriter::new(&mut b, proto));
        let (v, n) = kv_resp::decode_value(&b).unwrap().unwrap();
        assert_eq!(n, b.len());
        v
    }

    // Byte-exact replies of Redis 7.0.15 to `COMMAND INFO <name>`.
    #[test]
    fn info_matches_redis_resp2() {
        assert_eq!(
            info("get", Proto::Resp2),
            &b"*1\r\n*10\r\n$3\r\nget\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n*3\r\n+@read\r\n+@string\r\n+@fast\r\n*0\r\n*1\r\n*6\r\n$5\r\nflags\r\n*2\r\n+RO\r\n+access\r\n$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n*2\r\n$5\r\nindex\r\n:1\r\n$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n*6\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n"[..]
        );
        assert_eq!(
            info("CLIENT|SETNAME", Proto::Resp2),
            &b"*1\r\n*10\r\n$14\r\nclient|setname\r\n:3\r\n*3\r\n+noscript\r\n+loading\r\n+stale\r\n:0\r\n:0\r\n:0\r\n*2\r\n+@slow\r\n+@connection\r\n*0\r\n*0\r\n*0\r\n"[..]
        );
        assert_eq!(info("bogus", Proto::Resp2), b"*1\r\n$-1\r\n");
    }

    #[test]
    fn info_matches_redis_resp3() {
        assert_eq!(
            info("get", Proto::Resp3),
            &b"*1\r\n*10\r\n$3\r\nget\r\n:2\r\n~2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n~3\r\n+@read\r\n+@string\r\n+@fast\r\n~0\r\n~1\r\n%3\r\n$5\r\nflags\r\n~2\r\n+RO\r\n+access\r\n$12\r\nbegin_search\r\n%2\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n%1\r\n$5\r\nindex\r\n:1\r\n$9\r\nfind_keys\r\n%2\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n%3\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n~0\r\n"[..]
        );
        assert_eq!(
            info("set", Proto::Resp3),
            &b"*1\r\n*10\r\n$3\r\nset\r\n:-3\r\n~2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n~3\r\n+@write\r\n+@string\r\n+@slow\r\n~0\r\n~1\r\n%4\r\n$5\r\nnotes\r\n$48\r\nRW and ACCESS due to the optional `GET` argument\r\n$5\r\nflags\r\n~4\r\n+RW\r\n+access\r\n+update\r\n+variable_flags\r\n$12\r\nbegin_search\r\n%2\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n%1\r\n$5\r\nindex\r\n:1\r\n$9\r\nfind_keys\r\n%2\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n%3\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n~0\r\n"[..]
        );
        assert_eq!(
            info("flushall", Proto::Resp3),
            &b"*1\r\n*10\r\n$8\r\nflushall\r\n:-1\r\n~1\r\n+write\r\n:0\r\n:0\r\n:0\r\n~4\r\n+@keyspace\r\n+@write\r\n+@slow\r\n+@dangerous\r\n~2\r\n$25\r\nrequest_policy:all_shards\r\n$29\r\nresponse_policy:all_succeeded\r\n~0\r\n~0\r\n"[..]
        );
        assert_eq!(info("bogus", Proto::Resp3), b"*1\r\n_\r\n");
    }

    #[test]
    fn legacy_key_triples() {
        use kv_resp::Value::*;
        for (name, triple) in [
            ("mset", (1, -1, 2)),
            ("rename", (1, 2, 1)),
            ("copy", (1, 2, 1)),
            ("del", (1, -1, 1)),
            ("ping", (0, 0, 0)),
        ] {
            let v = run(Proto::Resp2, |w| {
                write_command_info_reply(&[name.as_bytes()], w)
            });
            let Array(Some(outer)) = v else { panic!() };
            let Array(Some(e)) = &outer[0] else { panic!() };
            assert_eq!(
                (&e[3], &e[4], &e[5]),
                (&Int(triple.0), &Int(triple.1), &Int(triple.2)),
                "{name}"
            );
        }
    }

    #[test]
    fn containers_nest_subcommands() {
        use kv_resp::Value::*;
        let v = run(Proto::Resp3, |w| write_command_info_reply(&[b"client"], w));
        let Array(Some(outer)) = v else { panic!() };
        let Array(Some(e)) = &outer[0] else { panic!() };
        let Array(Some(subs)) = &e[9] else { panic!() };
        assert_eq!(
            subs.len(),
            crate::lookup(b"client").unwrap().subcommands.len()
        );
        let v = run(Proto::Resp3, write_command_all);
        let Array(Some(all_entries)) = v else {
            panic!()
        };
        assert_eq!(all_entries.len(), command_count());
    }

    #[test]
    fn docs_shape() {
        use kv_resp::Value::*;
        let bulk = |s: &str| Bulk(Some(s.as_bytes().to_vec()));
        let v = run(Proto::Resp3, |w| {
            write_command_docs_reply(&[b"get", b"bogus"], w)
        });
        assert_eq!(
            v,
            Map(vec![(
                bulk("get"),
                Map(vec![
                    (bulk("summary"), bulk("Get the value of a key")),
                    (bulk("since"), bulk("1.0.0")),
                    (bulk("group"), bulk("string")),
                ])
            )])
        );
        // RESP2: flat arrays
        let v = run(Proto::Resp2, |w| write_command_docs_reply(&[b"get"], w));
        let Array(Some(flat)) = v else { panic!() };
        assert_eq!(flat.len(), 2);
        // containers carry their subcommands
        let v = run(Proto::Resp3, |w| write_command_docs_reply(&[b"config"], w));
        let Map(entries) = v else { panic!() };
        let Map(fields) = &entries[0].1 else { panic!() };
        assert_eq!(fields[3].0, bulk("subcommands"));
        let Map(subs) = &fields[3].1 else { panic!() };
        assert_eq!(subs[0].0, bulk("config|get"));
        let v = run(Proto::Resp2, |w| write_command_docs_reply(&[], w));
        let Array(Some(flat)) = v else { panic!() };
        assert_eq!(flat.len(), 2 * command_count());
    }

    #[test]
    fn list_filters() {
        use kv_resp::Value::*;
        let names = |args: &[&[u8]]| -> Vec<String> {
            match run(Proto::Resp2, |w| write_command_list_reply(args, w)) {
                Array(Some(v)) => v
                    .into_iter()
                    .map(|x| match x {
                        Bulk(Some(b)) => String::from_utf8(b).unwrap(),
                        _ => panic!(),
                    })
                    .collect(),
                Error(e) => vec![format!("ERROR {}", String::from_utf8_lossy(&e))],
                _ => panic!(),
            }
        };
        assert_eq!(names(&[]).len(), crate::CmdId::COUNT);
        let cl = names(&[b"FILTERBY", b"PATTERN", b"CL*"]);
        assert!(cl.contains(&"client|setname".to_string()) && cl.contains(&"cluster".to_string()));
        assert!(cl.iter().all(|n| n.starts_with("cl")));
        let conn = names(&[b"filterby", b"aclcat", b"CONNECTION"]);
        assert!(conn.contains(&"ping".to_string()) && conn.contains(&"command|info".to_string()));
        assert!(!conn.contains(&"get".to_string()));
        assert!(names(&[b"FILTERBY", b"ACLCAT", b"bogus"]).is_empty());
        assert!(names(&[b"FILTERBY", b"MODULE", b"x"]).is_empty());
        assert_eq!(
            names(&[b"FILTERBY", b"X", b"y"]),
            vec!["ERROR ERR syntax error"]
        );
        assert_eq!(names(&[b"FOO"]), vec!["ERROR ERR syntax error"]);
        assert_eq!(
            names(&[b"FILTERBY", b"PATTERN"]),
            vec!["ERROR ERR syntax error"]
        );
    }

    #[test]
    fn metadata_is_consistent_with_flags() {
        use crate::spec::CmdFlags;
        for spec in all_with_subcommands() {
            let m = meta(spec.id);
            assert_eq!(
                m.flags.contains(&"write"),
                spec.flags.contains(CmdFlags::WRITE),
                "{}",
                spec.name
            );
            assert_eq!(
                m.flags.contains(&"readonly"),
                spec.flags.contains(CmdFlags::READONLY),
                "{}",
                spec.name
            );
            assert_eq!(
                m.flags.contains(&"no_auth"),
                spec.flags.contains(CmdFlags::NO_AUTH),
                "{}",
                spec.name
            );
            assert_eq!(m.key_specs.is_empty(), !spec.has_keys(), "{}", spec.name);
            assert!(m.acl.iter().all(|c| c.starts_with('@')));
            for ks in m.key_specs {
                assert!(ks.index >= 1 && ks.keystep >= 1);
            }
        }
    }
}
