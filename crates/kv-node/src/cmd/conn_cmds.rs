//! Connection commands (DESIGN §7.4, §7.6, §8.2): PING, ECHO, HELLO (protocol switch, reply
//! in the new protocol with `server: kvd`, `version: 7.2.0`), AUTH (user `default` only),
//! SELECT (standalone `0..databases-1`; cluster: 0 only), QUIT, RESET and the CLIENT
//! subcommands (ID, GETNAME, SETNAME, INFO, LIST, SETINFO, NO-EVICT, NO-TOUCH, KILL, REPLY ON) in
//! Redis 7.0 formats.
