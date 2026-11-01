//! Operator API on `admin-port` (DESIGN §23.12).
//!
//! RESP `AUTH <admin-token>` then `KVADM <verb> ...`. Verbs map to `AdminOp` transitions
//! (params, node add/remove/maintenance/clear-fail, shard create/delete, slots assign, replica
//! add/remove/resync, migrate/pause/resume/abort, rebalance plan/apply) or to local operations
//! (`COORD SETPEERS`, `COORD PEERS`, export, status). On a follower, an admin command is relayed
//! to the leader and only the leader's reply text is returned.

use std::sync::Arc;

use crate::lease::LeaseTable;
use crate::push::Pusher;
use crate::wsr::Wsr;

/// The leader-side controllers the admin API drives.
pub struct Controllers {
    pub pusher: Arc<Pusher>,
    pub leases: tokio::sync::Mutex<LeaseTable>,
}

pub async fn serve_admin(l: tokio::net::TcpListener, wsr: Arc<Wsr>, ctl: Arc<Controllers>) -> std::io::Result<()> {
    todo!("DESIGN §23.12: admin service")
}
