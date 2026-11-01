//! Command descriptors (DESIGN §8.1, §24.5).
//!
//! Arity follows Redis: `arity >= 0` means exactly `arity` arguments including the command
//! name, `arity < 0` means at least `-arity`. Key specs give the key argument positions;
//! [`Access`] is the per-key access mode (§8.1: `M` meta, `R` read, `W` write, `RW`
//! read-modify-write). [`CmdId`] has one variant per command and per subcommand that has its
//! own table entry; handlers are registered by id in `kv-node/src/cmd/mod.rs`.

/// Per-key access mode (§8.1). Defined in `kv-common` so that `kv-storage` can carry it without
/// depending on this crate (§24.2).
pub use kv_common::Access;

bitflags::bitflags! {
    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    pub struct CmdFlags: u32 {
        const WRITE = 1;
        const READONLY = 2;
        const ADMIN = 4;
        const FAST = 8;
        const LOADING_OK = 16;
        const NO_MULTI = 32;
        const MULTI_KEY = 64;
        const KEYLESS = 128;
        const DELETE = 256;
        const CONN = 512;
        const CLUSTER_ONLY = 1024;
        const COORD_OK = 2048;
        const NO_AUTH = 4096;
    }
}

/// Key argument positions of a command.
#[derive(Clone, Copy)]
pub enum KeySpec {
    None,
    /// Keys at `first, first + step, ...` up to `last` (negative: counted from the end, `-1` =
    /// last argument).
    Range { first: u16, last: i16, step: u16 },
    /// Command-specific key extraction.
    Custom(fn(&[&[u8]]) -> smallvec::SmallVec<[u16; 4]>),
}

/// One command table entry.
pub struct CommandSpec {
    pub id: CmdId,
    pub name: &'static str,
    pub arity: i16,
    pub flags: CmdFlags,
    pub keys: KeySpec,
    pub access: Access,
    pub group: &'static str,
    pub since: &'static str,
    pub summary: &'static str,
    pub subcommands: &'static [CommandSpec],
}

/// One variant per command (§8.2-§8.7) and per subcommand with its own table entry.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(u16)]
pub enum CmdId {
    // connection and server (§8.2)
    Ping,
    Echo,
    Hello,
    Auth,
    Select,
    Quit,
    Reset,
    Client,
    ClientId,
    ClientGetname,
    ClientSetname,
    ClientInfo,
    ClientList,
    ClientSetinfo,
    ClientNoEvict,
    ClientNoTouch,
    ClientKill,
    ClientReply,
    Command,
    CommandCount,
    CommandInfo,
    CommandDocs,
    CommandList,
    CommandGetkeys,
    Info,
    Config,
    ConfigGet,
    ConfigSet,
    ConfigResetstat,
    ConfigRewrite,
    Dbsize,
    Flushdb,
    Flushall,
    Time,
    Save,
    Bgsave,
    Lastsave,
    Shutdown,
    Wait,
    Waitaof,
    Touch,
    /// Answered with `-ERR replication is managed by kvadm`.
    Replicaof,
    /// Answered with `-ERR replication is managed by kvadm`.
    Slaveof,
    // strings (§8.3)
    Get,
    Set,
    Setnx,
    Setex,
    Psetex,
    Getset,
    Getdel,
    Getex,
    Mget,
    Mset,
    Msetnx,
    Append,
    Strlen,
    Getrange,
    Substr,
    Setrange,
    Incr,
    Decr,
    Incrby,
    Decrby,
    Incrbyfloat,
    // keys, expiry, iteration (§8.4)
    Del,
    Unlink,
    Exists,
    Type,
    Rename,
    Renamenx,
    Copy,
    Randomkey,
    Keys,
    Scan,
    Expire,
    Pexpire,
    Expireat,
    Pexpireat,
    Ttl,
    Pttl,
    Expiretime,
    Pexpiretime,
    Persist,
    // transactions (§8.5)
    Multi,
    Exec,
    Discard,
    Watch,
    Unwatch,
    // hashes (§8.6)
    Hset,
    Hsetnx,
    Hget,
    Hmget,
    Hmset,
    Hdel,
    Hgetall,
    Hkeys,
    Hvals,
    Hlen,
    Hexists,
    Hincrby,
    Hincrbyfloat,
    Hstrlen,
    // cluster (§8.7)
    Cluster,
    ClusterKeyslot,
    ClusterMyid,
    ClusterInfo,
    ClusterSlots,
    ClusterShards,
    ClusterNodes,
    ClusterReplicas,
    ClusterSlaves,
    ClusterLinks,
    ClusterMyshardid,
    ClusterCountkeysinslot,
    ClusterGetkeysinslot,
    ClusterMeet,
    ClusterAddslots,
    ClusterAddslotsrange,
    ClusterDelslots,
    ClusterDelslotsrange,
    ClusterSetslot,
    ClusterFailover,
    ClusterForget,
    ClusterReplicate,
    ClusterReset,
    ClusterFlushslots,
    ClusterBumpepoch,
    ClusterSaveconfig,
    ClusterSetConfigEpoch,
    ClusterCountFailureReports,
    Readonly,
    Readwrite,
    Asking,
}
