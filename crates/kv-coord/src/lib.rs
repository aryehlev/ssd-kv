//! `kv-coord`: the coordinator service `kvc` (DESIGN §23.1-§23.5, §23.9-§23.13, §24.12).
//!
//! A topology service and control plane: it stores no user data and has no keyspace or storage
//! layer (LC-38). Coordinators replicate the whole [`kv_topo::ClusterState`] among themselves
//! (WSR, [`wsr`]): Raft-style randomized election, depth-one proposals of the whole next state,
//! fixed A/B persistence halves, and an operator-set peer list kept outside the voted value. The
//! leader dials every registered node ([`push`]), grants leases ([`lease`]), detects failures
//! from missed renewals ([`detector`]), fails over by the static rule ([`failover`]), commits ISR
//! changes ([`isr`]), orchestrates operator migrations with position barriers ([`migration`]),
//! serves client topology and redirects ([`clientsvc`]) and the operator API ([`admin`],
//! [`rebalance`]). Side effects are produced only from committed state (§23.3 rule 6).
//! Runtime: tokio multi-thread.
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]
#![allow(dead_code, unused_variables)]

pub mod admin;
pub mod clientsvc;
pub mod config;
pub mod detector;
pub mod failover;
pub mod isr;
pub mod lease;
pub mod log;
pub mod migration;
pub mod push;
pub mod rebalance;
pub mod wsr;

pub use admin::{Controllers, serve_admin};
pub use clientsvc::serve_clients;
pub use failover::{Candidate, choose_primary, run_failover};
pub use lease::LeaseTable;
pub use push::Pusher;
pub use rebalance::rebalance_plan;
pub use wsr::Wsr;
