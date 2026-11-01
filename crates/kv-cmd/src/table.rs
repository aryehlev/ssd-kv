//! The static command table (DESIGN §8.1-§8.7, §24.5).
//!
//! One static array of [`CommandSpec`]s (container commands point to their own subcommand
//! arrays) and a 512-entry open-addressed index over the case-folded names, built at compile
//! time (no perfect-hash crate, nothing to initialize at startup). Lookups are ASCII
//! case-insensitive and exact in length, like Redis's command dictionary.
//!
//! Commands that kvssd does not implement are simply absent and get Redis's
//! `-ERR unknown command` reply; in particular the table never contains scripting, function,
//! sort or module commands (LC-17).
//!
//! Arity, groups, `since` versions and summaries are those of Redis 7.0.15 (Redis 7.2 for CLIENT
//! SETINFO, CLIENT NO-TOUCH, WAITAOF and CLUSTER MYSHARDID, which the spec adds), from Redis's
//! command metadata (BSD-3-Clause, see NOTICE); summaries that name the Redis product are
//! reworded. Per-key access modes follow §8.3/§8.4.

use crate::spec::{Access, CmdFlags, CmdId, CommandSpec, KeySpec};

/// `CmdFlags` union usable in a `static` initializer.
macro_rules! fl {
    () => {
        CmdFlags::empty()
    };
    ($($f:ident)|+) => {
        CmdFlags::from_bits_retain(0 $(| CmdFlags::$f.bits())+)
    };
}

const fn range(first: u16, last: i16, step: u16) -> KeySpec {
    KeySpec::Range { first, last, step }
}

static CLIENT_SUBCOMMANDS: [CommandSpec; 10] = [
    CommandSpec { id: CmdId::ClientId, name: "client|id", arity: 2, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "5.0.0",
        summary: "Returns the client ID for the current connection", subcommands: &[] },
    CommandSpec { id: CmdId::ClientGetname, name: "client|getname", arity: 2, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "2.6.9",
        summary: "Get the current connection name", subcommands: &[] },
    CommandSpec { id: CmdId::ClientSetname, name: "client|setname", arity: 3, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "2.6.9",
        summary: "Set the current connection name", subcommands: &[] },
    CommandSpec { id: CmdId::ClientInfo, name: "client|info", arity: 2, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "6.2.0",
        summary: "Returns information about the current client connection.", subcommands: &[] },
    CommandSpec { id: CmdId::ClientList, name: "client|list", arity: -2, flags: fl!(ADMIN | LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "2.4.0",
        summary: "Get the list of client connections", subcommands: &[] },
    CommandSpec { id: CmdId::ClientSetinfo, name: "client|setinfo", arity: -4, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "7.2.0",
        summary: "Set client or connection specific info", subcommands: &[] },
    CommandSpec { id: CmdId::ClientNoEvict, name: "client|no-evict", arity: 3, flags: fl!(ADMIN | LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "7.0.0",
        summary: "Set client eviction mode for the current connection", subcommands: &[] },
    CommandSpec { id: CmdId::ClientNoTouch, name: "client|no-touch", arity: 3, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "7.2.0",
        summary: "Controls whether commands sent by the client will alter the LRU/LFU of the keys they access.", subcommands: &[] },
    CommandSpec { id: CmdId::ClientKill, name: "client|kill", arity: -3, flags: fl!(ADMIN | LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "2.4.0",
        summary: "Kill the connection of a client", subcommands: &[] },
    CommandSpec { id: CmdId::ClientReply, name: "client|reply", arity: 3, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "3.2.0",
        summary: "Instruct the server whether to reply to commands", subcommands: &[] },
];

static COMMAND_SUBCOMMANDS: [CommandSpec; 5] = [
    CommandSpec { id: CmdId::CommandCount, name: "command|count", arity: 2, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "2.8.13",
        summary: "Get total number of commands", subcommands: &[] },
    CommandSpec { id: CmdId::CommandInfo, name: "command|info", arity: -2, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "2.8.13",
        summary: "Get array of specific command details, or all when no argument is given.", subcommands: &[] },
    CommandSpec { id: CmdId::CommandDocs, name: "command|docs", arity: -2, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "7.0.0",
        summary: "Get array of specific command documentation", subcommands: &[] },
    CommandSpec { id: CmdId::CommandList, name: "command|list", arity: -2, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "7.0.0",
        summary: "Get an array of command names", subcommands: &[] },
    CommandSpec { id: CmdId::CommandGetkeys, name: "command|getkeys", arity: -4, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "2.8.13",
        summary: "Extract keys given a full command", subcommands: &[] },
];

static CONFIG_SUBCOMMANDS: [CommandSpec; 4] = [
    CommandSpec { id: CmdId::ConfigGet, name: "config|get", arity: -3, flags: fl!(ADMIN | LOADING_OK | KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "2.0.0",
        summary: "Get the values of configuration parameters", subcommands: &[] },
    CommandSpec { id: CmdId::ConfigSet, name: "config|set", arity: -4, flags: fl!(ADMIN | LOADING_OK | KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "2.0.0",
        summary: "Set configuration parameters to the given values", subcommands: &[] },
    CommandSpec { id: CmdId::ConfigResetstat, name: "config|resetstat", arity: 2, flags: fl!(ADMIN | LOADING_OK | KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "2.0.0",
        summary: "Reset the stats returned by INFO", subcommands: &[] },
    CommandSpec { id: CmdId::ConfigRewrite, name: "config|rewrite", arity: 2, flags: fl!(ADMIN | LOADING_OK | KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "2.8.0",
        summary: "Rewrite the configuration file with the in memory configuration", subcommands: &[] },
];

static CLUSTER_SUBCOMMANDS: [CommandSpec; 27] = [
    CommandSpec { id: CmdId::ClusterKeyslot, name: "cluster|keyslot", arity: 3, flags: fl!(KEYLESS | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Returns the hash slot of the specified key", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterMyid, name: "cluster|myid", arity: 2, flags: fl!(KEYLESS | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Return the node id", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterInfo, name: "cluster|info", arity: 2, flags: fl!(KEYLESS | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Provides info about cluster node state", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterSlots, name: "cluster|slots", arity: 2, flags: fl!(KEYLESS | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Get array of Cluster slot to node mappings", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterShards, name: "cluster|shards", arity: 2, flags: fl!(KEYLESS | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "7.0.0",
        summary: "Get array of cluster slots to node mappings", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterNodes, name: "cluster|nodes", arity: 2, flags: fl!(KEYLESS | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Get Cluster config for the node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterReplicas, name: "cluster|replicas", arity: 3, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "5.0.0",
        summary: "List replica nodes of the specified master node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterSlaves, name: "cluster|slaves", arity: 3, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "List replica nodes of the specified master node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterLinks, name: "cluster|links", arity: 2, flags: fl!(KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "7.0.0",
        summary: "Returns a list of all TCP links to and from peer nodes in cluster", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterMyshardid, name: "cluster|myshardid", arity: 2, flags: fl!(KEYLESS | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "7.2.0",
        summary: "Return the node shard id", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterCountkeysinslot, name: "cluster|countkeysinslot", arity: 3, flags: fl!(KEYLESS | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Return the number of local keys in the specified hash slot", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterGetkeysinslot, name: "cluster|getkeysinslot", arity: 4, flags: fl!(KEYLESS | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Return local key names in the specified hash slot", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterMeet, name: "cluster|meet", arity: -4, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Force a node cluster to handshake with another node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterAddslots, name: "cluster|addslots", arity: -3, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Assign new hash slots to receiving node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterAddslotsrange, name: "cluster|addslotsrange", arity: -4, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "7.0.0",
        summary: "Assign new hash slots to receiving node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterDelslots, name: "cluster|delslots", arity: -3, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Set hash slots as unbound in receiving node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterDelslotsrange, name: "cluster|delslotsrange", arity: -4, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "7.0.0",
        summary: "Set hash slots as unbound in receiving node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterSetslot, name: "cluster|setslot", arity: -4, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Bind a hash slot to a specific node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterFailover, name: "cluster|failover", arity: -2, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Forces a replica to perform a manual failover of its master.", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterForget, name: "cluster|forget", arity: 3, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Remove a node from the nodes table", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterReplicate, name: "cluster|replicate", arity: 3, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Reconfigure a node as a replica of the specified master node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterReset, name: "cluster|reset", arity: -2, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Reset a cluster node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterFlushslots, name: "cluster|flushslots", arity: 2, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Delete a node's own slots information", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterBumpepoch, name: "cluster|bumpepoch", arity: 2, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Advance the cluster config epoch", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterSaveconfig, name: "cluster|saveconfig", arity: 2, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Forces the node to save cluster state on disk", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterSetConfigEpoch, name: "cluster|set-config-epoch", arity: 3, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Set the configuration epoch in a new node", subcommands: &[] },
    CommandSpec { id: CmdId::ClusterCountFailureReports, name: "cluster|count-failure-reports", arity: 3, flags: fl!(ADMIN | KEYLESS | CLUSTER_ONLY),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Return the number of failure reports active for a given node", subcommands: &[] },
];

static COMMANDS: [CommandSpec; 87] = [
    CommandSpec { id: CmdId::Ping, name: "ping", arity: -1, flags: fl!(FAST | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "1.0.0",
        summary: "Ping the server", subcommands: &[] },
    CommandSpec { id: CmdId::Echo, name: "echo", arity: 2, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "1.0.0",
        summary: "Echo the given string", subcommands: &[] },
    CommandSpec { id: CmdId::Hello, name: "hello", arity: -1, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN | COORD_OK | NO_AUTH),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "6.0.0",
        summary: "Handshake with the server", subcommands: &[] },
    CommandSpec { id: CmdId::Auth, name: "auth", arity: -2, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN | COORD_OK | NO_AUTH),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "1.0.0",
        summary: "Authenticate to the server", subcommands: &[] },
    CommandSpec { id: CmdId::Select, name: "select", arity: 2, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "1.0.0",
        summary: "Change the selected database for the current connection", subcommands: &[] },
    CommandSpec { id: CmdId::Quit, name: "quit", arity: -1, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN | COORD_OK | NO_AUTH),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "1.0.0",
        summary: "Close the connection", subcommands: &[] },
    CommandSpec { id: CmdId::Reset, name: "reset", arity: 1, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN | COORD_OK | NO_AUTH),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "6.2.0",
        summary: "Reset the connection", subcommands: &[] },
    CommandSpec { id: CmdId::Client, name: "client", arity: -2, flags: fl!(KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "connection", since: "2.4.0",
        summary: "A container for client connection commands", subcommands: &CLIENT_SUBCOMMANDS },
    CommandSpec { id: CmdId::Command, name: "command", arity: -1, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "2.8.13",
        summary: "Get array of command details", subcommands: &COMMAND_SUBCOMMANDS },
    CommandSpec { id: CmdId::Info, name: "info", arity: -1, flags: fl!(LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "1.0.0",
        summary: "Get information and statistics about the server", subcommands: &[] },
    CommandSpec { id: CmdId::Config, name: "config", arity: -2, flags: fl!(KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "2.0.0",
        summary: "A container for server configuration commands", subcommands: &CONFIG_SUBCOMMANDS },
    CommandSpec { id: CmdId::Dbsize, name: "dbsize", arity: 1, flags: fl!(READONLY | FAST | KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "1.0.0",
        summary: "Return the number of keys in the selected database", subcommands: &[] },
    CommandSpec { id: CmdId::Flushdb, name: "flushdb", arity: -1, flags: fl!(WRITE | KEYLESS | DELETE),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "1.0.0",
        summary: "Remove all keys from the current database", subcommands: &[] },
    CommandSpec { id: CmdId::Flushall, name: "flushall", arity: -1, flags: fl!(WRITE | KEYLESS | DELETE),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "1.0.0",
        summary: "Remove all keys from all databases", subcommands: &[] },
    CommandSpec { id: CmdId::Time, name: "time", arity: 1, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "2.6.0",
        summary: "Return the current server time", subcommands: &[] },
    CommandSpec { id: CmdId::Save, name: "save", arity: 1, flags: fl!(ADMIN | NO_MULTI | KEYLESS),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "1.0.0",
        summary: "Synchronously save the dataset to disk", subcommands: &[] },
    CommandSpec { id: CmdId::Bgsave, name: "bgsave", arity: -1, flags: fl!(ADMIN | KEYLESS),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "1.0.0",
        summary: "Asynchronously save the dataset to disk", subcommands: &[] },
    CommandSpec { id: CmdId::Lastsave, name: "lastsave", arity: 1, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "1.0.0",
        summary: "Get the UNIX time stamp of the last successful save to disk", subcommands: &[] },
    CommandSpec { id: CmdId::Shutdown, name: "shutdown", arity: -1, flags: fl!(ADMIN | LOADING_OK | NO_MULTI | KEYLESS),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "1.0.0",
        summary: "Synchronously save the dataset to disk and then shut down the server", subcommands: &[] },
    CommandSpec { id: CmdId::Wait, name: "wait", arity: 3, flags: fl!(KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "generic", since: "3.0.0",
        summary: "Wait for the synchronous replication of all the write commands sent in the context of the current connection", subcommands: &[] },
    CommandSpec { id: CmdId::Waitaof, name: "waitaof", arity: 4, flags: fl!(KEYLESS),
        keys: KeySpec::None, access: Access::Meta, group: "generic", since: "7.2.0",
        summary: "Wait for all write commands sent in the context of the current connection to be durable on the local node and/or replicas", subcommands: &[] },
    CommandSpec { id: CmdId::Touch, name: "touch", arity: -2, flags: fl!(READONLY | FAST | MULTI_KEY),
        keys: range(1, -1, 1), access: Access::Meta, group: "generic", since: "3.2.1",
        summary: "Alters the last access time of a key(s). Returns the number of existing keys specified.", subcommands: &[] },
    CommandSpec { id: CmdId::Replicaof, name: "replicaof", arity: 3, flags: fl!(ADMIN | KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "5.0.0",
        summary: "Make the server a replica of another instance, or promote it as master.", subcommands: &[] },
    CommandSpec { id: CmdId::Slaveof, name: "slaveof", arity: 3, flags: fl!(ADMIN | KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "server", since: "1.0.0",
        summary: "Make the server a replica of another instance, or promote it as master.", subcommands: &[] },
    CommandSpec { id: CmdId::Get, name: "get", arity: 2, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Read, group: "string", since: "1.0.0",
        summary: "Get the value of a key", subcommands: &[] },
    CommandSpec { id: CmdId::Set, name: "set", arity: -3, flags: fl!(WRITE),
        keys: range(1, 1, 1), access: Access::Write, group: "string", since: "1.0.0",
        summary: "Set the string value of a key", subcommands: &[] },
    CommandSpec { id: CmdId::Setnx, name: "setnx", arity: 3, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::Write, group: "string", since: "1.0.0",
        summary: "Set the value of a key, only if the key does not exist", subcommands: &[] },
    CommandSpec { id: CmdId::Setex, name: "setex", arity: 4, flags: fl!(WRITE),
        keys: range(1, 1, 1), access: Access::Write, group: "string", since: "2.0.0",
        summary: "Set the value and expiration of a key", subcommands: &[] },
    CommandSpec { id: CmdId::Psetex, name: "psetex", arity: 4, flags: fl!(WRITE),
        keys: range(1, 1, 1), access: Access::Write, group: "string", since: "2.6.0",
        summary: "Set the value and expiration in milliseconds of a key", subcommands: &[] },
    CommandSpec { id: CmdId::Getset, name: "getset", arity: 3, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "string", since: "1.0.0",
        summary: "Set the string value of a key and return its old value", subcommands: &[] },
    CommandSpec { id: CmdId::Getdel, name: "getdel", arity: 2, flags: fl!(WRITE | FAST | DELETE),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "string", since: "6.2.0",
        summary: "Get the value of a key and delete the key", subcommands: &[] },
    CommandSpec { id: CmdId::Getex, name: "getex", arity: -2, flags: fl!(WRITE | FAST | DELETE),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "string", since: "6.2.0",
        summary: "Get the value of a key and optionally set its expiration", subcommands: &[] },
    CommandSpec { id: CmdId::Mget, name: "mget", arity: -2, flags: fl!(READONLY | FAST | MULTI_KEY),
        keys: range(1, -1, 1), access: Access::Read, group: "string", since: "1.0.0",
        summary: "Get the values of all the given keys", subcommands: &[] },
    CommandSpec { id: CmdId::Mset, name: "mset", arity: -3, flags: fl!(WRITE | MULTI_KEY),
        keys: range(1, -1, 2), access: Access::Write, group: "string", since: "1.0.1",
        summary: "Set multiple keys to multiple values", subcommands: &[] },
    CommandSpec { id: CmdId::Msetnx, name: "msetnx", arity: -3, flags: fl!(WRITE | MULTI_KEY),
        keys: range(1, -1, 2), access: Access::ReadWrite, group: "string", since: "1.0.1",
        summary: "Set multiple keys to multiple values, only if none of the keys exist", subcommands: &[] },
    CommandSpec { id: CmdId::Append, name: "append", arity: 3, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "string", since: "2.0.0",
        summary: "Append a value to a key", subcommands: &[] },
    CommandSpec { id: CmdId::Strlen, name: "strlen", arity: 2, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Meta, group: "string", since: "2.2.0",
        summary: "Get the length of the value stored in a key", subcommands: &[] },
    CommandSpec { id: CmdId::Getrange, name: "getrange", arity: 4, flags: fl!(READONLY),
        keys: range(1, 1, 1), access: Access::Read, group: "string", since: "2.4.0",
        summary: "Get a substring of the string stored at a key", subcommands: &[] },
    CommandSpec { id: CmdId::Substr, name: "substr", arity: 4, flags: fl!(READONLY),
        keys: range(1, 1, 1), access: Access::Read, group: "string", since: "1.0.0",
        summary: "Get a substring of the string stored at a key", subcommands: &[] },
    CommandSpec { id: CmdId::Setrange, name: "setrange", arity: 4, flags: fl!(WRITE),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "string", since: "2.2.0",
        summary: "Overwrite part of a string at key starting at the specified offset", subcommands: &[] },
    CommandSpec { id: CmdId::Incr, name: "incr", arity: 2, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "string", since: "1.0.0",
        summary: "Increment the integer value of a key by one", subcommands: &[] },
    CommandSpec { id: CmdId::Decr, name: "decr", arity: 2, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "string", since: "1.0.0",
        summary: "Decrement the integer value of a key by one", subcommands: &[] },
    CommandSpec { id: CmdId::Incrby, name: "incrby", arity: 3, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "string", since: "1.0.0",
        summary: "Increment the integer value of a key by the given amount", subcommands: &[] },
    CommandSpec { id: CmdId::Decrby, name: "decrby", arity: 3, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "string", since: "1.0.0",
        summary: "Decrement the integer value of a key by the given number", subcommands: &[] },
    CommandSpec { id: CmdId::Incrbyfloat, name: "incrbyfloat", arity: 3, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "string", since: "2.6.0",
        summary: "Increment the float value of a key by the given amount", subcommands: &[] },
    CommandSpec { id: CmdId::Del, name: "del", arity: -2, flags: fl!(WRITE | MULTI_KEY | DELETE),
        keys: range(1, -1, 1), access: Access::Meta, group: "generic", since: "1.0.0",
        summary: "Delete a key", subcommands: &[] },
    CommandSpec { id: CmdId::Unlink, name: "unlink", arity: -2, flags: fl!(WRITE | FAST | MULTI_KEY | DELETE),
        keys: range(1, -1, 1), access: Access::Meta, group: "generic", since: "4.0.0",
        summary: "Delete a key asynchronously in another thread. Otherwise it is just as DEL, but non blocking.", subcommands: &[] },
    CommandSpec { id: CmdId::Exists, name: "exists", arity: -2, flags: fl!(READONLY | FAST | MULTI_KEY),
        keys: range(1, -1, 1), access: Access::Meta, group: "generic", since: "1.0.0",
        summary: "Determine if a key exists", subcommands: &[] },
    CommandSpec { id: CmdId::Type, name: "type", arity: 2, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Meta, group: "generic", since: "1.0.0",
        summary: "Determine the type stored at key", subcommands: &[] },
    CommandSpec { id: CmdId::Rename, name: "rename", arity: 3, flags: fl!(WRITE | MULTI_KEY | DELETE),
        keys: range(1, 2, 1), access: Access::ReadWrite, group: "generic", since: "1.0.0",
        summary: "Rename a key", subcommands: &[] },
    CommandSpec { id: CmdId::Renamenx, name: "renamenx", arity: 3, flags: fl!(WRITE | FAST | MULTI_KEY | DELETE),
        keys: range(1, 2, 1), access: Access::ReadWrite, group: "generic", since: "1.0.0",
        summary: "Rename a key, only if the new key does not exist", subcommands: &[] },
    CommandSpec { id: CmdId::Copy, name: "copy", arity: -3, flags: fl!(WRITE | MULTI_KEY),
        keys: range(1, 2, 1), access: Access::Read, group: "generic", since: "6.2.0",
        summary: "Copy a key", subcommands: &[] },
    CommandSpec { id: CmdId::Randomkey, name: "randomkey", arity: 1, flags: fl!(READONLY | KEYLESS),
        keys: KeySpec::None, access: Access::Meta, group: "generic", since: "1.0.0",
        summary: "Return a random key from the keyspace", subcommands: &[] },
    CommandSpec { id: CmdId::Keys, name: "keys", arity: 2, flags: fl!(READONLY | KEYLESS),
        keys: KeySpec::None, access: Access::Meta, group: "generic", since: "1.0.0",
        summary: "Find all keys matching the given pattern", subcommands: &[] },
    CommandSpec { id: CmdId::Scan, name: "scan", arity: -2, flags: fl!(READONLY | KEYLESS),
        keys: KeySpec::None, access: Access::Meta, group: "generic", since: "2.8.0",
        summary: "Incrementally iterate the keys space", subcommands: &[] },
    CommandSpec { id: CmdId::Expire, name: "expire", arity: -3, flags: fl!(WRITE | FAST | DELETE),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "generic", since: "1.0.0",
        summary: "Set a key's time to live in seconds", subcommands: &[] },
    CommandSpec { id: CmdId::Pexpire, name: "pexpire", arity: -3, flags: fl!(WRITE | FAST | DELETE),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "generic", since: "2.6.0",
        summary: "Set a key's time to live in milliseconds", subcommands: &[] },
    CommandSpec { id: CmdId::Expireat, name: "expireat", arity: -3, flags: fl!(WRITE | FAST | DELETE),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "generic", since: "1.2.0",
        summary: "Set the expiration for a key as a UNIX timestamp", subcommands: &[] },
    CommandSpec { id: CmdId::Pexpireat, name: "pexpireat", arity: -3, flags: fl!(WRITE | FAST | DELETE),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "generic", since: "2.6.0",
        summary: "Set the expiration for a key as a UNIX timestamp specified in milliseconds", subcommands: &[] },
    CommandSpec { id: CmdId::Ttl, name: "ttl", arity: 2, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Meta, group: "generic", since: "1.0.0",
        summary: "Get the time to live for a key in seconds", subcommands: &[] },
    CommandSpec { id: CmdId::Pttl, name: "pttl", arity: 2, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Meta, group: "generic", since: "2.6.0",
        summary: "Get the time to live for a key in milliseconds", subcommands: &[] },
    CommandSpec { id: CmdId::Expiretime, name: "expiretime", arity: 2, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Meta, group: "generic", since: "7.0.0",
        summary: "Get the expiration Unix timestamp for a key", subcommands: &[] },
    CommandSpec { id: CmdId::Pexpiretime, name: "pexpiretime", arity: 2, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Meta, group: "generic", since: "7.0.0",
        summary: "Get the expiration Unix timestamp for a key in milliseconds", subcommands: &[] },
    CommandSpec { id: CmdId::Persist, name: "persist", arity: 2, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "generic", since: "2.2.0",
        summary: "Remove the expiration from a key", subcommands: &[] },
    CommandSpec { id: CmdId::Multi, name: "multi", arity: 1, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "transactions", since: "1.2.0",
        summary: "Mark the start of a transaction block", subcommands: &[] },
    CommandSpec { id: CmdId::Exec, name: "exec", arity: 1, flags: fl!(LOADING_OK | KEYLESS | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "transactions", since: "1.2.0",
        summary: "Execute all commands issued after MULTI", subcommands: &[] },
    CommandSpec { id: CmdId::Discard, name: "discard", arity: 1, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "transactions", since: "2.0.0",
        summary: "Discard all commands issued after MULTI", subcommands: &[] },
    CommandSpec { id: CmdId::Watch, name: "watch", arity: -2, flags: fl!(FAST | LOADING_OK | MULTI_KEY),
        keys: range(1, -1, 1), access: Access::Meta, group: "transactions", since: "2.2.0",
        summary: "Watch the given keys to determine execution of the MULTI/EXEC block", subcommands: &[] },
    CommandSpec { id: CmdId::Unwatch, name: "unwatch", arity: 1, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN),
        keys: KeySpec::None, access: Access::Meta, group: "transactions", since: "2.2.0",
        summary: "Forget about all watched keys", subcommands: &[] },
    CommandSpec { id: CmdId::Hset, name: "hset", arity: -4, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "hash", since: "2.0.0",
        summary: "Set the string value of a hash field", subcommands: &[] },
    CommandSpec { id: CmdId::Hsetnx, name: "hsetnx", arity: 4, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "hash", since: "2.0.0",
        summary: "Set the value of a hash field, only if the field does not exist", subcommands: &[] },
    CommandSpec { id: CmdId::Hget, name: "hget", arity: 3, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Read, group: "hash", since: "2.0.0",
        summary: "Get the value of a hash field", subcommands: &[] },
    CommandSpec { id: CmdId::Hmget, name: "hmget", arity: -3, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Read, group: "hash", since: "2.0.0",
        summary: "Get the values of all the given hash fields", subcommands: &[] },
    CommandSpec { id: CmdId::Hmset, name: "hmset", arity: -4, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "hash", since: "2.0.0",
        summary: "Set multiple hash fields to multiple values", subcommands: &[] },
    CommandSpec { id: CmdId::Hdel, name: "hdel", arity: -3, flags: fl!(WRITE | FAST | DELETE),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "hash", since: "2.0.0",
        summary: "Delete one or more hash fields", subcommands: &[] },
    CommandSpec { id: CmdId::Hgetall, name: "hgetall", arity: 2, flags: fl!(READONLY),
        keys: range(1, 1, 1), access: Access::Read, group: "hash", since: "2.0.0",
        summary: "Get all the fields and values in a hash", subcommands: &[] },
    CommandSpec { id: CmdId::Hkeys, name: "hkeys", arity: 2, flags: fl!(READONLY),
        keys: range(1, 1, 1), access: Access::Read, group: "hash", since: "2.0.0",
        summary: "Get all the fields in a hash", subcommands: &[] },
    CommandSpec { id: CmdId::Hvals, name: "hvals", arity: 2, flags: fl!(READONLY),
        keys: range(1, 1, 1), access: Access::Read, group: "hash", since: "2.0.0",
        summary: "Get all the values in a hash", subcommands: &[] },
    CommandSpec { id: CmdId::Hlen, name: "hlen", arity: 2, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Read, group: "hash", since: "2.0.0",
        summary: "Get the number of fields in a hash", subcommands: &[] },
    CommandSpec { id: CmdId::Hexists, name: "hexists", arity: 3, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Read, group: "hash", since: "2.0.0",
        summary: "Determine if a hash field exists", subcommands: &[] },
    CommandSpec { id: CmdId::Hincrby, name: "hincrby", arity: 4, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "hash", since: "2.0.0",
        summary: "Increment the integer value of a hash field by the given number", subcommands: &[] },
    CommandSpec { id: CmdId::Hincrbyfloat, name: "hincrbyfloat", arity: 4, flags: fl!(WRITE | FAST),
        keys: range(1, 1, 1), access: Access::ReadWrite, group: "hash", since: "2.6.0",
        summary: "Increment the float value of a hash field by the given amount", subcommands: &[] },
    CommandSpec { id: CmdId::Hstrlen, name: "hstrlen", arity: 3, flags: fl!(READONLY | FAST),
        keys: range(1, 1, 1), access: Access::Read, group: "hash", since: "3.2.0",
        summary: "Get the length of the value of a hash field", subcommands: &[] },
    CommandSpec { id: CmdId::Cluster, name: "cluster", arity: -2, flags: fl!(KEYLESS | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "A container for cluster commands", subcommands: &CLUSTER_SUBCOMMANDS },
    CommandSpec { id: CmdId::Readonly, name: "readonly", arity: 1, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Enables read queries for a connection to a cluster replica node", subcommands: &[] },
    CommandSpec { id: CmdId::Readwrite, name: "readwrite", arity: 1, flags: fl!(FAST | LOADING_OK | KEYLESS | CONN | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Disables read queries for a connection to a cluster replica node", subcommands: &[] },
    CommandSpec { id: CmdId::Asking, name: "asking", arity: 1, flags: fl!(FAST | KEYLESS | CONN | CLUSTER_ONLY | COORD_OK),
        keys: KeySpec::None, access: Access::Meta, group: "cluster", since: "3.0.0",
        summary: "Sent by cluster clients after an -ASK redirect", subcommands: &[] },
];

/// Size of the name index (a power of two, at least 4x the number of top-level commands).
const INDEX_SIZE: usize = 512;

/// Case-folded FNV-1a over a command name; `b | 0x20` folds ASCII letters (other bytes may
/// collide, which the exact comparison after the probe resolves).
#[inline]
const fn name_hash(name: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < name.len() {
        h = (h ^ (name[i] | 0x20) as u32).wrapping_mul(0x0100_0193);
        i += 1;
    }
    h
}

/// Open-addressed index: slot -> 1 + position in `COMMANDS` (0 = empty).
static INDEX: [u16; INDEX_SIZE] = {
    let mut idx = [0u16; INDEX_SIZE];
    let mut c = 0;
    while c < COMMANDS.len() {
        let mut slot = name_hash(COMMANDS[c].name.as_bytes()) as usize & (INDEX_SIZE - 1);
        while idx[slot] != 0 {
            slot = (slot + 1) & (INDEX_SIZE - 1);
        }
        idx[slot] = c as u16 + 1;
        c += 1;
    }
    idx
};

/// Longest top-level command name.
const MAX_NAME: usize = {
    let mut m = 0;
    let mut c = 0;
    while c < COMMANDS.len() {
        if COMMANDS[c].name.len() > m {
            m = COMMANDS[c].name.len();
        }
        c += 1;
    }
    m
};

/// Looks up a top-level command by name (ASCII case-insensitive).
#[inline]
pub fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    if name.is_empty() || name.len() > MAX_NAME {
        return None;
    }
    let mut slot = name_hash(name) as usize & (INDEX_SIZE - 1);
    loop {
        let e = INDEX[slot];
        if e == 0 {
            return None;
        }
        let spec = &COMMANDS[usize::from(e - 1)];
        if spec.name.len() == name.len() && spec.name.as_bytes().eq_ignore_ascii_case(name) {
            return Some(spec);
        }
        slot = (slot + 1) & (INDEX_SIZE - 1);
    }
}

/// Looks up a subcommand of a container command (CLIENT, COMMAND, CONFIG, CLUSTER), ASCII
/// case-insensitive. `None` for a command without subcommands.
pub fn lookup_sub(spec: &'static CommandSpec, sub: &[u8]) -> Option<&'static CommandSpec> {
    spec.subcommands.iter().find(|s| {
        let short = s.short_name().as_bytes();
        short.len() == sub.len() && short.eq_ignore_ascii_case(sub)
    })
}

/// Looks up a command by its full name as COMMAND INFO / COMMAND DOCS accept it: `get`,
/// `client` or `client|setname` (one level of subcommands, Redis `lookupCommandBySds`).
pub fn lookup_fullname(name: &[u8]) -> Option<&'static CommandSpec> {
    let mut parts = name.split(|&b| b == b'|');
    let base = lookup(parts.next()?)?;
    match (parts.next(), parts.next()) {
        (None, _) => Some(base),
        (Some(sub), None) => lookup_sub(base, sub),
        (Some(_), Some(_)) => None,
    }
}

/// Every top-level command, in table order.
pub fn all() -> &'static [CommandSpec] {
    &COMMANDS
}

/// Every entry: top-level commands followed, for each container, by its subcommands.
pub fn all_with_subcommands() -> impl Iterator<Item = &'static CommandSpec> {
    COMMANDS.iter().flat_map(|c| core::iter::once(c).chain(c.subcommands.iter()))
}

/// Table entry of each `CmdId`, indexed by discriminant: (position in `COMMANDS`, position in
/// its subcommand array + 1, or 0 for a top-level command).
static BY_ID: [(u8, u8); CmdId::COUNT] = {
    let mut t = [(u8::MAX, 0u8); CmdId::COUNT];
    let mut c = 0;
    while c < COMMANDS.len() {
        t[COMMANDS[c].id as usize] = (c as u8, 0);
        let subs = COMMANDS[c].subcommands;
        let mut s = 0;
        while s < subs.len() {
            t[subs[s].id as usize] = (c as u8, s as u8 + 1);
            s += 1;
        }
        c += 1;
    }
    let mut i = 0;
    while i < CmdId::COUNT {
        assert!(t[i].0 != u8::MAX, "every CmdId has a table entry");
        i += 1;
    }
    t
};

/// The table entry of `id`.
#[inline]
pub fn by_id(id: CmdId) -> &'static CommandSpec {
    let (c, s) = BY_ID[id as usize];
    let top = &COMMANDS[usize::from(c)];
    if s == 0 { top } else { &top.subcommands[usize::from(s - 1)] }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookups() {
        assert_eq!(lookup(b"get").unwrap().id, CmdId::Get);
        assert_eq!(lookup(b"GeT").unwrap().id, CmdId::Get);
        assert_eq!(lookup(b"PEXPIRETIME").unwrap().id, CmdId::Pexpiretime);
        assert!(lookup(b"").is_none());
        assert!(lookup(b"get\x00").is_none());
        assert!(lookup(b"gett").is_none());
        assert!(lookup(b"setname").is_none(), "subcommands are not top-level");
        assert!(lookup(b"G\x05T").is_none());
        let client = lookup(b"client").unwrap();
        assert!(client.is_container());
        assert_eq!(lookup_sub(client, b"SetName").unwrap().id, CmdId::ClientSetname);
        assert_eq!(lookup_sub(client, b"no-evict").unwrap().id, CmdId::ClientNoEvict);
        assert!(lookup_sub(client, b"bogus").is_none());
        assert!(lookup_sub(lookup(b"get").unwrap(), b"x").is_none());
        assert_eq!(lookup_fullname(b"CLIENT|SETNAME").unwrap().id, CmdId::ClientSetname);
        assert_eq!(lookup_fullname(b"client").unwrap().id, CmdId::Client);
        assert!(lookup_fullname(b"get|x").is_none());
        assert!(lookup_fullname(b"client|").is_none());
        assert!(lookup_fullname(b"a|b|c").is_none());
        assert!(lookup_fullname(b"").is_none());
    }

    #[test]
    fn every_entry_is_reachable() {
        let mut seen = 0;
        for spec in all_with_subcommands() {
            seen += 1;
            assert!(core::ptr::eq(by_id(spec.id), spec), "{}", spec.name);
            assert_eq!(spec.name, spec.name.to_ascii_lowercase());
            match spec.name.split_once('|') {
                None => assert!(core::ptr::eq(lookup(spec.name.to_uppercase().as_bytes()).unwrap(), spec)),
                Some((base, _)) => {
                    let parent = lookup(base.as_bytes()).unwrap();
                    assert!(core::ptr::eq(lookup_sub(parent, spec.short_name().as_bytes()).unwrap(), spec));
                    assert!(spec.subcommands.is_empty(), "one level of subcommands");
                }
            }
            assert_eq!(spec.flags.contains(CmdFlags::KEYLESS), !spec.has_keys(), "{}", spec.name);
            assert!(!(spec.flags.contains(CmdFlags::WRITE) && spec.flags.contains(CmdFlags::READONLY)));
            if !spec.has_keys() {
                assert_eq!(spec.access, Access::Meta, "{}", spec.name);
            }
        }
        assert_eq!(seen, CmdId::COUNT, "one entry per CmdId");
        assert_eq!(all().len(), 87);
    }

    #[test]
    fn no_excluded_commands() {
        // §1.2 / §8.2: never in the table (the LC-17 names are enforced by ci/invariant_lint.sh)
        for name in ["swapdb", "move", "object", "memory", "monitor", "debug", "sync", "psync", "migrate", "subscribe",
            "publish", "lpush", "zadd", "sadd", "xadd", "bgrewriteaof", "acl", "pubsub", "latency"]
        {
            assert!(lookup(name.as_bytes()).is_none(), "{name}");
        }
    }
}
