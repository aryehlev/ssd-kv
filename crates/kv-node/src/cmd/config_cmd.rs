//! CONFIG GET / SET / RESETSTAT (DESIGN §20.2). CONFIG GET also answers the compatibility
//! keys clients query (`save ""`, `appendonly no`, `maxmemory 0`, `maxmemory-policy
//! noeviction`, `cluster-enabled`, `databases`); unknown patterns give an empty array. CONFIG
//! REWRITE replies `-ERR CONFIG REWRITE not supported`.
//!
//! CONFIG SET of several parameters is all-or-nothing (Redis 7.0): every pair is applied to a
//! copy of the configuration, which replaces the node's only when all of them succeed.

use kv_resp::ReplyWriter;

use super::{Args, ConnCtx, ConnOutcome, write_arity_error};

/// `CONFIG` without a subcommand (normally answered by command resolution).
pub fn config_container(
    _cx: &mut ConnCtx<'_>,
    _a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
) -> ConnOutcome {
    write_arity_error("config", w);
    ConnOutcome::Done
}

/// Whether a CONFIG GET argument is a glob (Redis looks names without `[*?` up directly and
/// replies with the name as the client wrote it).
fn is_pattern(p: &[u8]) -> bool {
    p.iter().any(|b| matches!(b, b'[' | b'*' | b'?'))
}

/// CONFIG GET pattern [pattern ...]: a map of every matching parameter (each listed once).
pub fn config_get(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    let cfg = cx.shared.config();
    let mut out: Vec<(Vec<u8>, String)> = Vec::new();
    for &pat in &a.argv[2..] {
        for (name, value) in cfg.get(pat) {
            if out
                .iter()
                .any(|(n, _)| n.eq_ignore_ascii_case(name.as_bytes()))
            {
                continue;
            }
            let key = if is_pattern(pat) {
                name.into_bytes()
            } else {
                pat.to_vec()
            };
            out.push((key, value));
        }
    }
    w.map(out.len());
    for (k, v) in &out {
        w.bulk(k);
        w.bulk(v.as_bytes());
    }
    ConnOutcome::Done
}

/// CONFIG SET name value [name value ...].
pub fn config_set(cx: &mut ConnCtx<'_>, a: &Args<'_>, w: &mut ReplyWriter<'_>) -> ConnOutcome {
    if a.len() % 2 == 1 {
        write_arity_error("config|set", w);
        return ConnOutcome::Done;
    }
    let mut pairs: Vec<(&str, &str)> = Vec::with_capacity((a.len() - 2) / 2);
    for pair in a.argv[2..].chunks_exact(2) {
        let name = String::from_utf8_lossy(super::c_str(pair[0]));
        if !crate::config::Config::is_parameter(&name) {
            let mut m = b"ERR Unknown option or number of arguments for CONFIG SET - '".to_vec();
            m.extend_from_slice(super::c_str(pair[0]));
            m.push(b'\'');
            w.error(&m);
            return ConnOutcome::Done;
        }
        let (Ok(name), Ok(value)) = (std::str::from_utf8(pair[0]), std::str::from_utf8(pair[1]))
        else {
            let mut m = b"ERR CONFIG SET failed (possibly related to argument '".to_vec();
            m.extend_from_slice(super::c_str(pair[0]));
            m.extend_from_slice(b"') - argument is not valid UTF-8");
            w.error(&m);
            return ConnOutcome::Done;
        };
        if pairs.iter().any(|(n, _)| n.eq_ignore_ascii_case(name)) {
            let mut m = b"ERR CONFIG SET failed (possibly related to argument '".to_vec();
            m.extend_from_slice(name.as_bytes());
            m.extend_from_slice(b"') - duplicate parameter");
            w.error(&m);
            return ConnOutcome::Done;
        }
        pairs.push((name, value));
    }
    match cx
        .shared
        .update_config(|c| pairs.iter().try_for_each(|(n, v)| c.set(n, v)))
    {
        Ok(()) => w.ok(),
        Err(e) => {
            let mut m = b"ERR ".to_vec();
            m.extend_from_slice(e.0.as_bytes());
            w.error(&m);
        }
    }
    ConnOutcome::Done
}

/// CONFIG RESETSTAT: clears the command statistics and latency histograms of every worker.
/// Keyspace hit/miss counters belong to the partitions and keep counting.
pub fn config_resetstat(
    cx: &mut ConnCtx<'_>,
    _a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
) -> ConnOutcome {
    for s in &cx.shared.command_stats {
        s.reset();
    }
    for l in &cx.shared.latency {
        l.reset();
    }
    w.ok();
    ConnOutcome::Done
}

/// CONFIG REWRITE: the configuration file is never written by the server.
pub fn config_rewrite(
    _cx: &mut ConnCtx<'_>,
    _a: &Args<'_>,
    w: &mut ReplyWriter<'_>,
) -> ConnOutcome {
    w.error(b"ERR CONFIG REWRITE not supported");
    ConnOutcome::Done
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::conn_cmds::tests::Fixture;

    #[test]
    fn config_get_replies() {
        let mut f = Fixture::new();
        assert_eq!(
            f.run_conn(config_get, "CONFIG GET MAXMEMORY"),
            "*2\r\n$9\r\nMAXMEMORY\r\n$1\r\n0\r\n"
        );
        assert_eq!(f.run_conn(config_get, "CONFIG GET nosuch"), "*0\r\n");
        let r = f.run_conn(config_get, "CONFIG GET maxmemory save appendonly");
        assert!(
            r.starts_with("*6\r\n")
                && r.contains("$4\r\nsave\r\n$0\r\n\r\n")
                && r.contains("$10\r\nappendonly\r\n$2\r\nno\r\n"),
            "{r}"
        );
        let r = f.run_conn(config_get, "CONFIG GET databases data*");
        assert!(r.contains("$9\r\ndatabases\r\n$2\r\n16\r\n"), "{r}");
        assert_eq!(r.matches("databases").count(), 1, "listed once: {r}");
        f.conn.proto = kv_resp::Proto::Resp3;
        assert_eq!(
            f.run_conn(config_get, "CONFIG GET maxmemory"),
            "%1\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n"
        );
    }

    #[test]
    fn config_set_replies() {
        let mut f = Fixture::new();
        assert_eq!(
            f.run_conn(config_set, "CONFIG SET maxclients"),
            "-ERR wrong number of arguments for 'config|set' command\r\n"
        );
        assert_eq!(
            f.run_conn(config_set, "CONFIG SET maxclients 100 MAXCLIENTS 200"),
            "-ERR CONFIG SET failed (possibly related to argument 'MAXCLIENTS') - duplicate parameter\r\n"
        );
        assert_eq!(
            f.run_conn(config_set, "CONFIG SET foo bar"),
            "-ERR Unknown option or number of arguments for CONFIG SET - 'foo'\r\n"
        );
        assert_eq!(
            f.run_conn(config_set, "CONFIG SET maxclients 123"),
            "+OK\r\n"
        );
        assert_eq!(f.shared.config().maxclients, 123);
        let r = f.run_conn(config_set, "CONFIG SET maxclients 7 maxclients-x 1");
        assert!(r.starts_with("-ERR Unknown option"), "{r}");
        let r = f.run_conn(config_set, "CONFIG SET maxclients 7 port 1");
        assert!(
            r.starts_with("-ERR CONFIG SET failed (possibly related to argument 'port')"),
            "{r}"
        );
        assert_eq!(f.shared.config().maxclients, 123, "all-or-nothing");
        let r = f.run_conn(config_set, "CONFIG SET maxclients abc");
        assert!(
            r.starts_with("-ERR CONFIG SET failed (possibly related to argument 'maxclients') - "),
            "{r}"
        );
    }

    #[test]
    fn resetstat_and_rewrite() {
        let mut f = Fixture::new();
        f.shared.command_stats[0].record(kv_cmd::CmdId::Get, 5, false);
        assert_eq!(f.run_conn(config_resetstat, "CONFIG RESETSTAT"), "+OK\r\n");
        assert_eq!(f.shared.command_stats[0].get(kv_cmd::CmdId::Get).calls, 0);
        assert_eq!(
            f.run_conn(config_rewrite, "CONFIG REWRITE"),
            "-ERR CONFIG REWRITE not supported\r\n"
        );
    }
}
