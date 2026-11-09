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
    /// kvssd dispatch properties of a command. The Redis flags reported by COMMAND INFO are kept
    /// separately (they are Redis's, verbatim); these are the ones the data node and the
    /// coordinator act on.
    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    pub struct CmdFlags: u32 {
        /// May modify the keyspace (Redis `write`).
        const WRITE = 1;
        /// Reads the keyspace and never modifies it (Redis `readonly`).
        const READONLY = 2;
        /// Administrative command (Redis `admin`).
        const ADMIN = 4;
        /// O(1) or O(log n) command (Redis `fast`).
        const FAST = 8;
        /// Allowed while the node is loading (Redis `loading`); others get `-LOADING`.
        const LOADING_OK = 16;
        /// Refused inside MULTI (Redis `no_multi`).
        const NO_MULTI = 32;
        /// Takes more than one key (MGET, MSET, MSETNX, DEL, UNLINK, EXISTS, TOUCH, RENAME,
        /// RENAMENX, COPY, WATCH): subject to the same-slot rule in cluster mode (§23.14) and to
        /// the per-partition split of §6.8.
        const MULTI_KEY = 64;
        /// Has no key arguments.
        const KEYLESS = 128;
        /// May delete a key, including through an expiry in the past (DEL, UNLINK, GETDEL,
        /// GETEX, EXPIRE family, RENAME family, HDEL, FLUSHDB, FLUSHALL): the reply waits for
        /// the durable-delete rule (§16.2).
        const DELETE = 256;
        /// Connection or server command answered on the connection's home worker without a
        /// partition request.
        const CONN = 512;
        /// Valid only in cluster mode (CLUSTER, READONLY, READWRITE, ASKING); standalone nodes
        /// answer `-ERR This instance has cluster support disabled` (§8.7).
        const CLUSTER_ONLY = 1024;
        /// Answered by the coordinator itself (§23.5): protocol, connection and topology
        /// commands. Keyed commands get MOVED there instead.
        const COORD_OK = 2048;
        /// Allowed before authentication (Redis `no_auth`: AUTH, HELLO, QUIT, RESET).
        const NO_AUTH = 4096;
    }
}

/// Key argument positions of a command.
#[derive(Clone, Copy)]
pub enum KeySpec {
    None,
    /// Keys at `first, first + step, ...` up to `last` (negative: counted from the end, `-1` =
    /// last argument). This is the legacy `(first, last, step)` triple of COMMAND INFO.
    Range {
        first: u16,
        last: i16,
        step: u16,
    },
    /// Command-specific key extraction.
    Custom(fn(&[&[u8]]) -> smallvec::SmallVec<[u16; 4]>),
}

/// One command table entry. `name` is the Redis "full name": lowercase, and
/// `container|subcommand` for subcommands (as in COMMAND INFO and in arity errors).
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

impl CommandSpec {
    /// Container commands (CLIENT, COMMAND, CONFIG, CLUSTER) dispatch on their first argument.
    #[inline]
    pub fn is_container(&self) -> bool {
        !self.subcommands.is_empty()
    }

    /// The name after `|` for a subcommand, else the whole name.
    pub fn short_name(&self) -> &'static str {
        match self.name.split_once('|') {
            Some((_, sub)) => sub,
            None => self.name,
        }
    }

    /// Whether the command has key arguments.
    #[inline]
    pub fn has_keys(&self) -> bool {
        !matches!(self.keys, KeySpec::None)
    }
}

impl core::fmt::Debug for CommandSpec {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CommandSpec")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("arity", &self.arity)
            .finish_non_exhaustive()
    }
}

/// One variant per command (§8.2-§8.7) and per subcommand with its own table entry. The
/// discriminants are dense, in declaration order (`CmdId::COUNT` of them).
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

impl CmdId {
    /// Number of variants.
    pub const COUNT: usize = CmdId::Asking as usize + 1;

    /// This command's table entry.
    #[inline]
    pub fn spec(self) -> &'static CommandSpec {
        crate::table::by_id(self)
    }
}
