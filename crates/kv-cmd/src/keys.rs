//! Command resolution, key positions, arity and the same-slot rule (DESIGN §6.8, §7.5, §8.1,
//! §23.6, §23.14).
//!
//! * [`resolve`] maps an argv to its table entry the way Redis 7.0's `processCommand` does
//!   (container subcommands, then the arity check) and [`ResolveError::write`] renders Redis's
//!   exact error replies (`unknown command`, `unknown subcommand`, `wrong number of arguments`).
//! * [`key_positions`] / [`key_access`] give the key arguments and their per-key access modes
//!   (§8.1); [`common_slot`] is used by the data node's router (cluster mode: keys in more than
//!   one slot give `-CROSSSLOT Keys in request don't hash to the same slot`) and by the
//!   coordinator to compute `-MOVED <slot> <primary>` for keyed commands.
//! * [`write_command_getkeys`] answers COMMAND GETKEYS.

use smallvec::SmallVec;

use crate::spec::{Access, CmdId, CommandSpec, KeySpec};
use crate::table::{lookup, lookup_sub};

/// Argument indexes (into `argv`, 0 = command name) of the command's keys. Like Redis, a key
/// range that does not fit `argv` yields no keys (the arity check reports the error).
pub fn key_positions(spec: &CommandSpec, argv: &[&[u8]]) -> SmallVec<[u16; 4]> {
    match spec.keys {
        KeySpec::None => SmallVec::new(),
        KeySpec::Custom(f) => f(argv),
        KeySpec::Range { first, last, step } => {
            let argc = argv.len() as i64;
            let first = i64::from(first);
            let last = if last >= 0 { i64::from(last) } else { argc + i64::from(last) };
            if first == 0 || first >= argc || last >= argc || last < first || step == 0 {
                return SmallVec::new();
            }
            (first..=last).step_by(usize::from(step)).map(|i| i as u16).collect()
        }
    }
}

/// Access mode of the key at argument index `pos` (§8.1): the table's mode, refined where the
/// spec gives a command-specific one: SET is `ReadWrite` with GET, KEEPTTL, NX or XX; RENAME /
/// RENAMENX read-modify-write their source and blindly write their destination; COPY reads its
/// source and writes its destination.
pub fn key_access(spec: &CommandSpec, argv: &[&[u8]], pos: u16) -> Access {
    match spec.id {
        CmdId::Set => {
            let reads = argv.iter().skip(3).any(|a| {
                [&b"GET"[..], b"KEEPTTL", b"NX", b"XX"].iter().any(|o| a.eq_ignore_ascii_case(o))
            });
            if reads { Access::ReadWrite } else { Access::Write }
        }
        CmdId::Rename | CmdId::Renamenx => if pos == 1 { Access::ReadWrite } else { Access::Write },
        CmdId::Copy => if pos == 1 { Access::Read } else { Access::Write },
        _ => spec.access,
    }
}

/// Redis arity rule: `arity >= 0` exact, `arity < 0` minimum `-arity` (command name included).
pub fn check_arity(spec: &CommandSpec, argc: usize) -> bool {
    if spec.arity >= 0 {
        argc == spec.arity as usize
    } else {
        argc >= usize::from(spec.arity.unsigned_abs())
    }
}

/// The slot shared by the keys at `pos`, `None` for a keyless command, `Err(CrossSlot)` when
/// the keys map to more than one slot.
pub fn common_slot(argv: &[&[u8]], pos: &[u16]) -> Result<Option<u16>, CrossSlot> {
    let mut slot = None;
    for &p in pos {
        let s = kv_common::key_slot(argv[usize::from(p)]);
        match slot {
            None => slot = Some(s),
            Some(prev) if prev != s => return Err(CrossSlot),
            Some(_) => {}
        }
    }
    Ok(slot)
}

/// Keys of one request map to different slots.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CrossSlot;

impl CrossSlot {
    /// The Redis error reply text (argument of `ReplyWriter::error`).
    pub const MESSAGE: &'static [u8] = b"CROSSSLOT Keys in request don't hash to the same slot";
}

/// Why an argv does not name an executable command.
#[derive(Clone, Copy, Debug)]
pub enum ResolveError {
    /// `argv[0]` is not a command (or `argv` is empty).
    UnknownCommand,
    /// `argv[0]` is a container command and `argv[1]` is not one of its subcommands.
    UnknownSubcommand,
    /// The command exists but `argv.len()` violates its arity.
    WrongArity(&'static CommandSpec),
}

/// Redis `lookupCommand` (non-strict): the container's subcommand when `argv` has a second
/// argument, else the command itself.
fn lookup_argv(argv: &[&[u8]]) -> Option<&'static CommandSpec> {
    let base = lookup(argv.first()?)?;
    if argv.len() == 1 || !base.is_container() { Some(base) } else { lookup_sub(base, argv[1]) }
}

/// Resolves `argv` (argv[0] = command name) to its table entry exactly as Redis 7.0 does
/// before executing a command: a container command with arguments resolves to its
/// subcommand, then the arity is checked (a container without arguments is checked against
/// its own arity, so `CLIENT` alone is an arity error and `COMMAND` alone is valid).
pub fn resolve(argv: &[&[u8]]) -> Result<&'static CommandSpec, ResolveError> {
    let Some(spec) = lookup_argv(argv) else {
        return Err(match argv.first().and_then(|n| lookup(n)) {
            Some(base) if base.is_container() => ResolveError::UnknownSubcommand,
            _ => ResolveError::UnknownCommand,
        });
    };
    if check_arity(spec, argv.len()) { Ok(spec) } else { Err(ResolveError::WrongArity(spec)) }
}

/// A C string view: the bytes before the first NUL (Redis formats these arguments with `%s`).
fn c_str(s: &[u8]) -> &[u8] {
    match s.iter().position(|&b| b == 0) {
        Some(n) => &s[..n],
        None => s,
    }
}

impl ResolveError {
    /// Writes Redis 7.0's error reply for this failure (`argv` as given to [`resolve`]).
    /// CR and LF bytes of echoed arguments are written as spaces, as Redis does.
    pub fn write(&self, argv: &[&[u8]], w: &mut kv_resp::ReplyWriter<'_>) {
        let mut m = Vec::with_capacity(160);
        self.message(argv, &mut m);
        w.error(&m);
    }

    /// The error text (without the leading `-` and CRLF; CR/LF not yet replaced).
    pub fn message(&self, argv: &[&[u8]], out: &mut Vec<u8>) {
        match *self {
            ResolveError::UnknownCommand => {
                out.extend_from_slice(b"ERR unknown command '");
                let name = argv.first().map(|n| c_str(n)).unwrap_or(b"");
                out.extend_from_slice(&name[..name.len().min(128)]);
                out.extend_from_slice(b"', with args beginning with: ");
                // Redis: while the list is shorter than 128 bytes, append "'%.*s' " with the
                // remaining budget as precision
                let mut list_len = 0usize;
                for a in argv.iter().skip(1) {
                    if list_len >= 128 {
                        break;
                    }
                    let a = c_str(a);
                    let take = a.len().min(128 - list_len);
                    out.push(b'\'');
                    out.extend_from_slice(&a[..take]);
                    out.extend_from_slice(b"' ");
                    list_len += take + 3;
                }
            }
            ResolveError::UnknownSubcommand => {
                out.extend_from_slice(b"ERR unknown subcommand '");
                let sub = argv.get(1).map(|s| c_str(s)).unwrap_or(b"");
                out.extend_from_slice(&sub[..sub.len().min(128)]);
                out.extend_from_slice(b"'. Try ");
                out.extend(argv.first().map(|n| c_str(n)).unwrap_or(b"").iter().map(u8::to_ascii_uppercase));
                out.extend_from_slice(b" HELP.");
            }
            ResolveError::WrongArity(spec) => write_arity_message(spec, out),
        }
    }
}

/// `ERR wrong number of arguments for '<full name>' command`.
pub fn write_arity_message(spec: &CommandSpec, out: &mut Vec<u8>) {
    out.extend_from_slice(b"ERR wrong number of arguments for '");
    out.extend_from_slice(spec.name.as_bytes());
    out.extend_from_slice(b"' command");
}

/// COMMAND GETKEYS: `target` is the command whose keys are wanted (the arguments after
/// `COMMAND GETKEYS`). Replies with the key arguments, or Redis 7.0's errors: `Invalid command
/// specified`, `The command has no key arguments`, `Invalid number of arguments specified for
/// command`, `Invalid arguments specified for command`.
pub fn write_command_getkeys(target: &[&[u8]], w: &mut kv_resp::ReplyWriter<'_>) {
    let Some(spec) = lookup_argv(target) else {
        return w.error(b"ERR Invalid command specified");
    };
    if !spec.has_keys() {
        return w.error(b"ERR The command has no key arguments");
    }
    if !check_arity(spec, target.len()) {
        return w.error(b"ERR Invalid number of arguments specified for command");
    }
    let pos = key_positions(spec, target);
    if pos.is_empty() {
        return w.error(b"ERR Invalid arguments specified for command");
    }
    w.array(pos.len());
    for p in pos {
        w.bulk(target[usize::from(p)]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv_resp::{Proto, ReplyWriter};

    fn spec(name: &str) -> &'static CommandSpec {
        crate::lookup_fullname(name.as_bytes()).unwrap()
    }

    fn pos(argv: &[&str]) -> Vec<u16> {
        let a: Vec<&[u8]> = argv.iter().map(|s| s.as_bytes()).collect();
        key_positions(resolve(&a).unwrap(), &a).to_vec()
    }

    fn reply(f: impl FnOnce(&mut ReplyWriter<'_>)) -> Vec<u8> {
        let mut b = Vec::new();
        f(&mut ReplyWriter::new(&mut b, Proto::Resp2));
        b
    }

    fn err(argv: &[&[u8]]) -> Vec<u8> {
        let e = resolve(argv).unwrap_err();
        reply(|w| e.write(argv, w))
    }

    #[test]
    fn arity_vectors() {
        // (command, arity) from Redis 7.0.15 COMMAND INFO (7.2 for WAITAOF and CLIENT SETINFO)
        let table: &[(&str, i16)] = &[
            ("get", 2), ("set", -3), ("mset", -3), ("mget", -2), ("del", -2), ("exists", -2), ("incr", 2),
            ("incrby", 3), ("getrange", 4), ("setrange", 4), ("getex", -2), ("expire", -3), ("ttl", 2),
            ("rename", 3), ("copy", -3), ("scan", -2), ("keys", 2), ("ping", -1), ("echo", 2), ("hello", -1),
            ("auth", -2), ("select", 2), ("client", -2), ("client|setname", 3), ("client|kill", -3),
            ("client|setinfo", -4), ("command", -1), ("command|getkeys", -4), ("command|info", -2),
            ("config|set", -4), ("config|get", -3), ("flushdb", -1), ("shutdown", -1), ("wait", 3),
            ("waitaof", 4), ("hset", -4), ("hgetall", 2), ("cluster|keyslot", 3), ("multi", 1), ("exec", 1),
            ("watch", -2), ("info", -1), ("time", 1), ("readonly", 1), ("substr", 4), ("getset", 3),
        ];
        for &(name, arity) in table {
            assert_eq!(spec(name).arity, arity, "{name}");
        }
        assert!(check_arity(spec("get"), 2) && !check_arity(spec("get"), 3) && !check_arity(spec("get"), 1));
        assert!(check_arity(spec("set"), 3) && check_arity(spec("set"), 9) && !check_arity(spec("set"), 2));
    }

    #[test]
    fn key_spec_vectors() {
        assert_eq!(pos(&["GET", "k"]), [1]);
        assert_eq!(pos(&["SET", "k", "v", "EX", "10"]), [1]);
        assert_eq!(pos(&["MSET", "a", "1", "b", "2"]), [1, 3]);
        assert_eq!(pos(&["MSET", "a", "1", "b"]), [1, 3], "Redis's range over an odd tail");
        assert_eq!(pos(&["MGET", "a", "b", "c"]), [1, 2, 3]);
        assert_eq!(pos(&["DEL", "a"]), [1]);
        assert_eq!(pos(&["RENAME", "a", "b"]), [1, 2]);
        assert_eq!(pos(&["COPY", "a", "b", "DB", "1", "REPLACE"]), [1, 2]);
        assert_eq!(pos(&["MSETNX", "a", "1"]), [1]);
        assert_eq!(pos(&["WATCH", "a", "b"]), [1, 2]);
        assert_eq!(pos(&["HSET", "h", "f", "v"]), [1]);
        assert_eq!(pos(&["PING"]), Vec::<u16>::new());
        assert_eq!(pos(&["SCAN", "0", "MATCH", "x"]), Vec::<u16>::new());
        assert_eq!(pos(&["CLIENT", "SETNAME", "x"]), Vec::<u16>::new());
        // ranges that do not fit argv yield nothing
        let short: [&[u8]; 1] = [b"RENAME"];
        assert!(key_positions(spec("rename"), &short).is_empty());
    }

    #[test]
    fn access_modes() {
        let a = |argv: &[&str], p: u16| {
            let v: Vec<&[u8]> = argv.iter().map(|s| s.as_bytes()).collect();
            key_access(resolve(&v).unwrap(), &v, p)
        };
        assert_eq!(a(&["GET", "k"], 1), Access::Read);
        assert_eq!(a(&["SET", "k", "v"], 1), Access::Write);
        assert_eq!(a(&["SET", "k", "v", "EX", "5"], 1), Access::Write);
        assert_eq!(a(&["SET", "k", "v", "nx"], 1), Access::ReadWrite);
        assert_eq!(a(&["SET", "k", "v", "get"], 1), Access::ReadWrite);
        assert_eq!(a(&["SET", "k", "v", "KeepTTL"], 1), Access::ReadWrite);
        assert_eq!(a(&["RENAME", "a", "b"], 1), Access::ReadWrite);
        assert_eq!(a(&["RENAME", "a", "b"], 2), Access::Write);
        assert_eq!(a(&["COPY", "a", "b"], 1), Access::Read);
        assert_eq!(a(&["COPY", "a", "b"], 2), Access::Write);
        assert_eq!(a(&["EXISTS", "a"], 1), Access::Meta);
        assert_eq!(a(&["INCR", "a"], 1), Access::ReadWrite);
        assert_eq!(a(&["MSETNX", "a", "1"], 1), Access::ReadWrite);
        assert_eq!(a(&["STRLEN", "a"], 1), Access::Meta);
    }

    #[test]
    fn same_slot_rule() {
        let argv: [&[u8]; 5] = [b"MSET", b"{u}a", b"1", b"{u}b", b"2"];
        assert_eq!(common_slot(&argv, &[1, 3]), Ok(Some(kv_common::key_slot(b"u"))));
        let argv: [&[u8]; 3] = [b"MGET", b"foo", b"bar"];
        assert_eq!(common_slot(&argv, &[1, 2]), Err(CrossSlot));
        assert_eq!(common_slot(&argv, &[1, 1]), Ok(Some(12182)));
        assert_eq!(common_slot(&argv, &[]), Ok(None));
    }

    #[test]
    fn resolution_errors_match_redis() {
        // texts captured from Redis 7.0.15
        assert_eq!(err(&[b"FOO", b"a", b"b"]), b"-ERR unknown command 'FOO', with args beginning with: 'a' 'b' \r\n");
        assert_eq!(err(&[b"FOO"]), b"-ERR unknown command 'FOO', with args beginning with: \r\n");
        assert_eq!(err(&[b"FO\r\nO", b"a\nb"]), b"-ERR unknown command 'FO  O', with args beginning with: 'a b' \r\n");
        assert_eq!(err(&[b"FO\x00O", b"a\x00b"]), b"-ERR unknown command 'FO', with args beginning with: 'a' \r\n");
        let long = vec![b'x'; 200];
        let mut want = b"-ERR unknown command 'FOO', with args beginning with: '".to_vec();
        want.extend_from_slice(&[b'x'; 128]);
        want.extend_from_slice(b"' \r\n");
        assert_eq!(err(&[b"FOO", &long, b"yyyy"]), want);
        let name = vec![b'n'; 300];
        let e = err(&[&name]);
        assert!(e.starts_with(b"-ERR unknown command 'nnnn") && e.windows(129).all(|w| w != [b'n'; 129]));
        // budget shared across arguments
        let a100 = vec![b'a'; 100];
        let mut want = b"-ERR unknown command 'X', with args beginning with: '".to_vec();
        want.extend_from_slice(&a100);
        want.extend_from_slice(b"' '");
        want.extend_from_slice(&[b'b'; 25]);
        want.extend_from_slice(b"' \r\n");
        assert_eq!(err(&[b"X", &a100, &[b'b'; 40], b"c"]), want);
        assert_eq!(err(&[b"CLIENT", b"BOGUS", b"x"]), b"-ERR unknown subcommand 'BOGUS'. Try CLIENT HELP.\r\n");
        assert_eq!(err(&[b"client", b"bo\x00gs"]), b"-ERR unknown subcommand 'bo'. Try CLIENT HELP.\r\n");
        assert_eq!(err(&[b"Command", b"bogus"]), b"-ERR unknown subcommand 'bogus'. Try COMMAND HELP.\r\n");
        assert_eq!(err(&[b"CLIENT", b"SETNAME"]), b"-ERR wrong number of arguments for 'client|setname' command\r\n");
        assert_eq!(err(&[b"CLIENT"]), b"-ERR wrong number of arguments for 'client' command\r\n");
        assert_eq!(err(&[b"get"]), b"-ERR wrong number of arguments for 'get' command\r\n");
        assert_eq!(err(&[]), b"-ERR unknown command '', with args beginning with: \r\n");
        assert_eq!(resolve(&[b"COMMAND"]).unwrap().id, CmdId::Command);
        assert_eq!(resolve(&[b"get", b"k"]).unwrap().id, CmdId::Get);
        assert_eq!(resolve(&[b"PING", b"x"]).unwrap().id, CmdId::Ping, "no subcommands: extra args are arguments");
    }

    #[test]
    fn getkeys_matches_redis() {
        let gk = |t: &[&[u8]]| reply(|w| write_command_getkeys(t, w));
        assert_eq!(gk(&[b"mset", b"a", b"1", b"b"]), b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
        assert_eq!(gk(&[b"rename", b"a", b"b"]), b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
        assert_eq!(gk(&[b"bogus", b"x"]), b"-ERR Invalid command specified\r\n");
        assert_eq!(gk(&[b"client", b"bogus"]), b"-ERR Invalid command specified\r\n");
        assert_eq!(gk(&[b"ping", b"x"]), b"-ERR The command has no key arguments\r\n");
        assert_eq!(gk(&[b"client", b"setname", b"x"]), b"-ERR The command has no key arguments\r\n");
        assert_eq!(gk(&[b"command", b"getkeys", b"get", b"a"]), b"-ERR The command has no key arguments\r\n");
        assert_eq!(gk(&[b"get", b"a", b"b"]), b"-ERR Invalid number of arguments specified for command\r\n");
    }
}
