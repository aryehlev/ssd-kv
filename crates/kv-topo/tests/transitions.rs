//! Unit tests of `ClusterState::apply`, the node projection, failover helpers and the encoding
//! (DESIGN §23.2, §23.4, §23.9, §23.12, §23.13).

mod common;

use std::net::SocketAddr;

use common::*;
use kv_ctrl::{
    CoordEndpoint, CoordId, MemberState, MigPhase, MigrationTask, PeerAddr, ShardId, SlotRanges,
    TermEnd,
};
use kv_topo::redirect::{self, KeysRedirect, Redirect};
use kv_topo::state::{
    MAX_DONE_MIGRATIONS, MAX_STATE_BYTES, NodeFlags, STATE_FORMAT_VERSION, ShardStatus, StaticRank,
    static_choice,
};
use kv_topo::{
    AdminOp, ClusterParams, ClusterState, Member, NodeRecord, Reject, ShardRecord, Transition,
};

const S1: ShardId = ShardId(1);
const S2: ShardId = ShardId(2);
const S3: ShardId = ShardId(3);

fn takeover() -> Vec<u64> {
    vec![0; 64]
}

fn none(_: &kv_ctrl::NodeId) -> bool {
    false
}

fn all(_: &kv_ctrl::NodeId) -> bool {
    true
}

fn is_pre(r: Result<ClusterState, Reject>) -> bool {
    matches!(r, Err(Reject::Precondition(_)))
}

#[test]
fn initial_state() {
    let s = ClusterState::initial(ClusterParams::default(), [1; 16]);
    assert_eq!(s.version, 1);
    assert_eq!(s.assigned_slots(), 0);
    assert!(s.nodes.is_empty() && s.shards.is_empty() && s.migrations.is_empty());
    assert!(s.shard_of_slot(0).is_none());
    let b = s.encode();
    assert_eq!(b[0], STATE_FORMAT_VERSION);
    assert!(ClusterState::decode(&b).unwrap() == s);
    assert_eq!(s.check_invariants(), Ok(()));
}

#[test]
fn versions_and_empty_batch() {
    let s = three_by_two();
    let n = apply_ok(&s, &[]);
    assert_eq!(n.version, s.version + 1);
    let n2 = apply_ok(
        &n,
        &[Transition::Health {
            node: nid(4),
            fail: true,
        }],
    );
    assert_eq!(n2.version, n.version + 1);
}

#[test]
fn batch_is_all_or_nothing() {
    let s = three_by_two();
    let before = s.encode();
    let r = s.apply(&[
        Transition::Health {
            node: nid(4),
            fail: true,
        },
        admin(AdminOp::ShardDelete { id: ShardId(99) }),
    ]);
    assert_eq!(r.err(), Some(Reject::UnknownShard));
    assert_eq!(s.encode(), before, "apply is pure");
}

#[test]
fn node_add_preconditions() {
    let s = three_by_two();
    assert!(is_pre(s.apply(&[node_add(1, 0)])), "duplicate id");
    assert!(
        is_pre(s.apply(&[node_add_at(nid(50), client_addr(1), 0)])),
        "duplicate client addr"
    );
    let wrong_p = admin(AdminOp::NodeAdd {
        id: nid(51),
        ctrl: "10.9.0.1:1".parse().unwrap(),
        client: "10.9.0.1:2".parse().unwrap(),
        repl: "10.9.0.1:3".parse().unwrap(),
        zone: "a".into(),
        priority: 0,
        partitions: 32,
    });
    assert!(is_pre(s.apply(&[wrong_p])), "partition count must match");
    let long_zone = admin(AdminOp::NodeAdd {
        id: nid(52),
        ctrl: "10.9.0.2:1".parse().unwrap(),
        client: "10.9.0.2:2".parse().unwrap(),
        repl: "10.9.0.2:3".parse().unwrap(),
        zone: "z".repeat(65),
        priority: 0,
        partitions: 64,
    });
    assert!(is_pre(s.apply(&[long_zone])));
    let n = apply_ok(&s, &[node_add(7, 3)]);
    let rec = n.node(&nid(7)).unwrap();
    assert_eq!(rec.added_version, n.version);
    assert_eq!(rec.flags, NodeFlags::default());
    assert!(
        n.assignment_for(&nid(7), 1, &[]).role.is_none(),
        "a new node is a standby"
    );
}

#[test]
fn params() {
    let s = ClusterState::initial(ClusterParams::default(), [0; 16]);
    let set = |s: &ClusterState, k: &str, v: &str| {
        s.apply(&[admin(AdminOp::ParamSet {
            key: k.into(),
            value: v.into(),
        })])
    };
    let n = set(&s, "lease-ms", "3000").unwrap();
    assert_eq!(n.params.lease_ms, 3000);
    let n = set(&n, "partitions", "128").unwrap();
    assert_eq!(n.params.partitions, 128);
    assert!(is_pre(set(&n, "partitions", "100")));
    assert!(is_pre(set(&n, "lease_ms", "abc")));
    assert!(
        is_pre(set(&n, "renew_ms", "5000")),
        "lease must be >= 3 x renew"
    );
    assert!(is_pre(set(&n, "no_such", "1")));
    let n = set(&n, "requirepass", "s3cret-pw").unwrap();
    assert_eq!(n.params.client_password.as_deref(), Some("s3cret-pw"));
    for dbg in [format!("{n:?}"), format!("{:?}", n.params)] {
        assert!(
            !dbg.contains("s3cret-pw"),
            "Debug leaks the password: {dbg}"
        );
    }
    let t = admin(AdminOp::ParamSet {
        key: "client_password".into(),
        value: "s3cret-pw".into(),
    });
    assert!(!format!("{t:?}").contains("s3cret-pw"));
    let a = n.assignment_for(&nid(1), 1, &[]);
    assert_eq!(a.client_password.as_deref(), Some("s3cret-pw"));
    let n = set(&n, "client-password", "").unwrap();
    assert_eq!(n.params.client_password, None);
    let n = set(&n, "PARTITIONS", "64").unwrap();
    // partitions are fixed once nodes are registered
    let n = apply_ok(&n, &[node_add_at(nid(1), client_addr(1), 0)]);
    assert!(n.apply(&[node_add(1, 0)]).is_err());
    assert!(is_pre(set(&n, "partitions", "64")));
    assert_eq!(ClusterParams::default().validate(), Ok(()));
}

#[test]
fn shard_and_slot_preconditions() {
    let s = three_by_two();
    let n = apply_ok(&s, &[node_add(7, 0), node_add(8, 0)]);
    let create = |id: u16, p: u8, min_isr: u8| {
        admin(AdminOp::ShardCreate {
            id: ShardId(id),
            primary: nid(p),
            min_isr,
        })
    };
    assert!(is_pre(n.apply(&[create(0xFFFF, 7, 1)])), "reserved id");
    assert!(is_pre(n.apply(&[create(1, 7, 1)])), "existing shard");
    assert!(is_pre(n.apply(&[create(9, 4, 1)])), "node has a role");
    assert!(
        is_pre(n.apply(&[create(9, 7, 0)])),
        "min_isr counts the primary"
    );
    assert_eq!(
        n.apply(&[create(9, 99, 1)]).err(),
        Some(Reject::UnknownNode)
    );
    let n = apply_ok(&n, &[create(9, 7, 2)]);
    let sh = n.shard(ShardId(9)).unwrap();
    assert_eq!(
        (sh.term, sh.status, sh.min_isr),
        (1, ShardStatus::Serving, 2)
    );
    assert_eq!(
        sh.members,
        vec![Member {
            node: nid(7),
            state: MemberState::InSync,
            in_isr: true
        }]
    );
    // slots
    let assign = |a, b, sid| {
        admin(AdminOp::SlotsAssign {
            slots: ranges(a, b),
            shard: ShardId(sid),
        })
    };
    assert!(is_pre(n.apply(&[assign(5, 5, 9)])), "already assigned");
    assert!(is_pre(n.apply(&[admin(AdminOp::SlotsAssign {
        slots: SlotRanges(vec![(5, 1)]),
        shard: ShardId(9)
    })])));
    assert!(is_pre(n.apply(&[admin(AdminOp::SlotsAssign {
        slots: SlotRanges(vec![]),
        shard: ShardId(9)
    })])));
    // delete: only without slots and unfinished migrations
    assert!(is_pre(n.apply(&[admin(AdminOp::ShardDelete { id: S1 })])));
    let n = apply_ok(&n, &[admin(AdminOp::ShardDelete { id: ShardId(9) })]);
    assert!(n.shard(ShardId(9)).is_none());
    assert!(n.assignment_for(&nid(7), 1, &[]).role.is_none());
}

#[test]
fn replicas_add_remove_resync() {
    let s = three_by_two();
    let n = apply_ok(
        &s,
        &[
            node_add(7, 0),
            admin(AdminOp::ReplicaAdd {
                shard: S1,
                node: nid(7),
            }),
        ],
    );
    let m = *n.shard(S1).unwrap().member(&nid(7)).unwrap();
    assert_eq!(
        (m.state, m.in_isr),
        (MemberState::Syncing, false),
        "a learner starts syncing"
    );
    assert!(
        is_pre(n.apply(&[admin(AdminOp::ReplicaAdd {
            shard: S2,
            node: nid(7)
        })])),
        "one role per node"
    );
    assert!(
        is_pre(n.apply(&[admin(AdminOp::ReplicaRemove {
            shard: S1,
            node: nid(1)
        })])),
        "not the primary"
    );
    assert!(
        is_pre(n.apply(&[admin(AdminOp::ReplicaResync { node: nid(7) })])),
        "already syncing"
    );
    assert!(
        is_pre(n.apply(&[admin(AdminOp::ReplicaResync { node: nid(1) })])),
        "not the primary"
    );
    let n = apply_ok(
        &n,
        &[admin(AdminOp::ReplicaRemove {
            shard: S1,
            node: nid(7),
        })],
    );
    assert!(n.shard(S1).unwrap().member(&nid(7)).is_none());
    // resync of an in-sync member: allowed structurally, precondition helper decides
    assert!(n.resync_allowed(&nid(4), false).is_err());
    assert_eq!(n.resync_allowed(&nid(4), true), Ok(()));
    let n = apply_ok(
        &n,
        &[Transition::MemberState {
            shard: S1,
            node: nid(4),
            state: MemberState::NeedsFullSync,
        }],
    );
    assert_eq!(n.resync_allowed(&nid(4), false), Ok(()));
    assert!(
        !n.shard(S1).unwrap().member(&nid(4)).unwrap().in_isr,
        "NeedsFullSync leaves the ISR"
    );
    let n = apply_ok(&n, &[admin(AdminOp::ReplicaResync { node: nid(4) })]);
    assert_eq!(
        n.shard(S1).unwrap().member(&nid(4)).unwrap().state,
        MemberState::Syncing
    );
}

#[test]
fn no_automatic_full_sync() {
    let s = three_by_two();
    let r = s.apply(&[Transition::MemberState {
        shard: S1,
        node: nid(4),
        state: MemberState::Syncing,
    }]);
    assert!(
        matches!(r, Err(Reject::Precondition(_))),
        "only replica add/resync start a full sync"
    );
}

#[test]
fn node_remove_and_maintenance() {
    let s = three_by_two();
    assert!(is_pre(s.apply(&[admin(AdminOp::NodeRemove {
        id: nid(4),
        force: false
    })])));
    assert!(
        is_pre(s.apply(&[admin(AdminOp::NodeRemove {
            id: nid(4),
            force: true
        })])),
        "force needs FAIL"
    );
    let n = apply_ok(
        &s,
        &[Transition::Health {
            node: nid(4),
            fail: true,
        }],
    );
    let n = apply_ok(
        &n,
        &[admin(AdminOp::NodeRemove {
            id: nid(4),
            force: true,
        })],
    );
    assert!(n.node(&nid(4)).is_none() && n.shard(S1).unwrap().member(&nid(4)).is_none());
    let n = apply_ok(
        &n,
        &[Transition::Health {
            node: nid(1),
            fail: true,
        }],
    );
    assert!(
        is_pre(n.apply(&[admin(AdminOp::NodeRemove {
            id: nid(1),
            force: true
        })])),
        "still a primary"
    );
    let n = apply_ok(
        &n,
        &[
            node_add(9, 0),
            admin(AdminOp::NodeMaint {
                id: nid(9),
                on: true,
            }),
        ],
    );
    assert!(n.node(&nid(9)).unwrap().flags.maint);
    let n = apply_ok(
        &n,
        &[admin(AdminOp::NodeRemove {
            id: nid(9),
            force: false,
        })],
    );
    assert!(n.node(&nid(9)).is_none());
}

#[test]
fn assignment_projection() {
    let s = three_by_two();
    let coords = vec![
        CoordEndpoint {
            id: CoordId([1; 20]),
            client: "10.1.0.1:7000".parse().unwrap(),
        },
        CoordEndpoint {
            id: CoordId([2; 20]),
            client: "10.1.0.2:7000".parse().unwrap(),
        },
    ];
    let p = s.assignment_for(&nid(1), 5, &coords);
    assert_eq!((p.version, p.coord_term, p.node_id), (s.version, 5, nid(1)));
    assert_eq!(p.coordinators, coords);
    assert_eq!(p.params, s.params.node_params());
    let role = p.role.as_ref().unwrap();
    assert!(role.is_primary && role.shard == S1 && role.term == 1);
    assert_eq!(role.owned_slots.len(), 5461);
    assert!(
        role.owned_slots.contains(0)
            && role.owned_slots.contains(5460)
            && !role.owned_slots.contains(5461)
    );
    assert_eq!(role.primary_repl, None);
    assert_eq!(
        role.members,
        vec![(nid(4), MemberState::InSync, true)],
        "primary: its peers only"
    );
    let r = s.assignment_for(&nid(4), 5, &coords);
    let rrole = r.role.as_ref().unwrap();
    assert!(!rrole.is_primary);
    assert_eq!(
        rrole.primary_repl,
        Some(PeerAddr(s.node(&nid(1)).unwrap().repl))
    );
    assert!(
        rrole.members.is_empty() && rrole.migrations.is_empty(),
        "replicas learn no peers"
    );
    assert!(rrole.owned_slots == role.owned_slots);
    // the projection does not change when another shard changes
    let n = apply_ok(
        &s,
        &[Transition::Health {
            node: nid(6),
            fail: true,
        }],
    );
    assert!(n.assignment_for(&nid(1), 5, &coords).same_projection(&p));
    assert!(
        !n.assignment_for(&nid(1), 5, &coords)
            .same_projection(&s.assignment_for(&nid(2), 5, &coords))
    );
}

#[test]
fn health_and_status() {
    let s = three_by_two();
    let n = apply_ok(
        &s,
        &[Transition::Health {
            node: nid(1),
            fail: true,
        }],
    );
    assert_eq!(n.shard(S1).unwrap().status, ShardStatus::Promoting);
    assert_eq!(redirect::for_slot(&n, 0, &none), Redirect::ClusterDown);
    assert!(
        n.assignment_for(&nid(1), 1, &[])
            .role
            .is_some_and(|r| !r.is_primary),
        "no primary role while failed"
    );
    assert!(
        n.assignment_for(&nid(4), 1, &[])
            .role
            .is_some_and(|r| r.primary_repl.is_none())
    );
    // the only candidate also fails -> NoPrimary; it recovers -> Promoting again
    let n2 = apply_ok(
        &n,
        &[Transition::Health {
            node: nid(4),
            fail: true,
        }],
    );
    assert_eq!(n2.shard(S1).unwrap().status, ShardStatus::NoPrimary);
    let n3 = apply_ok(
        &n2,
        &[Transition::Health {
            node: nid(4),
            fail: false,
        }],
    );
    assert_eq!(n3.shard(S1).unwrap().status, ShardStatus::Promoting);
    // clearing the primary's flag does not resume service without a promotion
    let n4 = apply_ok(
        &n3,
        &[Transition::Health {
            node: nid(1),
            fail: false,
        }],
    );
    assert_eq!(n4.shard(S1).unwrap().status, ShardStatus::Promoting);
    assert_eq!(redirect::for_slot(&n4, 0, &none), Redirect::ClusterDown);
    assert_eq!(n4.nodes, {
        let mut v = s.nodes.clone();
        v.iter_mut().for_each(|r| r.flags.fail = false);
        v
    });
}

#[test]
fn redirects() {
    let s = three_by_two();
    let c1 = s.node(&nid(1)).unwrap().client;
    assert_eq!(redirect::for_slot(&s, 0, &none), Redirect::Moved(0, c1));
    assert_eq!(
        redirect::for_slot(&s, 0, &all),
        Redirect::ClusterDown,
        "PFAIL in leader RAM"
    );
    assert_eq!(redirect::for_slot(&s, 16384, &none), Redirect::ClusterDown);
    assert_eq!(
        redirect::for_slot(
            &ClusterState::initial(ClusterParams::default(), [0; 16]),
            5,
            &none
        ),
        Redirect::ClusterDown
    );
    assert_eq!(
        Redirect::Moved(12182, "127.0.0.1:30003".parse().unwrap()).error_text(),
        b"MOVED 12182 127.0.0.1:30003"
    );
    assert_eq!(
        redirect::moved_text(1, "[::1]:7000".parse().unwrap()),
        b"MOVED 1 ::1:7000"
    );
    assert_eq!(
        Redirect::ClusterDown.error_text(),
        b"CLUSTERDOWN Hash slot not served"
    );
    assert_eq!(
        Redirect::TryAgain.error_text(),
        b"TRYAGAIN Multiple keys request during rehashing of slot"
    );
    assert_eq!(redirect::for_key_slots(&s, &[], &none), None);
    assert_eq!(
        redirect::for_key_slots(&s, &[3, 4], &none),
        Some(KeysRedirect::CrossSlot)
    );
    assert_eq!(
        redirect::for_key_slots(&s, &[7, 7], &none),
        Some(KeysRedirect::Redirect(Redirect::Moved(7, c1)))
    );
    let mut out = Vec::new();
    KeysRedirect::CrossSlot.write(&mut kv_resp::ReplyWriter::new(
        &mut out,
        kv_resp::Proto::Resp2,
    ));
    assert_eq!(out, include_bytes!("golden/crossslot.bin"));
}

#[test]
fn static_choice_rule() {
    let r = |priority, order| StaticRank { priority, order };
    assert_eq!(static_choice(&[], &[]), None);
    assert_eq!(
        static_choice(&[r(1, 0), r(3, 2), r(3, 1)], &[]),
        Some(2),
        "priority, then member order"
    );
    assert_eq!(static_choice(&[r(1, 0), r(3, 2), r(3, 1)], &[2]), Some(1));
    assert_eq!(
        static_choice(&[r(1, 0), r(3, 2), r(3, 1)], &[0, 1, 2]),
        None
    );
}

/// Shard 1 with members 1 (primary), 4, 7, 8, all ISR; priorities 1, 4, 7, 8 -> order 8, 7, 4.
fn rf4() -> ClusterState {
    let s = three_by_two();
    let s = apply_ok(&s, &[node_add(7, 7), node_add(8, 8)]);
    let s = add_insync_replica(&s, S1, nid(7));
    add_insync_replica(&s, S1, nid(8))
}

#[test]
fn promotion_order_and_choice() {
    let s = rf4();
    assert_eq!(s.promotion_order(S1, &all), vec![nid(8), nid(7), nid(4)]);
    // lease table decides eligibility; maintenance only if no other
    let held = |n: &kv_ctrl::NodeId| *n != nid(8);
    assert_eq!(s.promotion_order(S1, &held), vec![nid(7), nid(4)]);
    let m = apply_ok(
        &s,
        &[admin(AdminOp::NodeMaint {
            id: nid(7),
            on: true,
        })],
    );
    assert_eq!(m.promotion_order(S1, &held), vec![nid(4), nid(7)]);
    assert_eq!(m.choose_primary(S1, &held, &[nid(4)]), Some(nid(7)));
    assert_eq!(m.choose_primary(S1, &held, &[nid(4), nid(7)]), None);
    // FAIL-flagged and non-ISR members are not eligible
    let f = apply_ok(
        &s,
        &[
            Transition::Health {
                node: nid(8),
                fail: true,
            },
            Transition::Isr {
                shard: S1,
                term: 1,
                node: nid(7),
                add: false,
            },
        ],
    );
    assert_eq!(f.promotion_order(S1, &all), vec![nid(4)]);
    assert!(s.promotion_order(ShardId(77), &all).is_empty());
    // equal priorities: earliest member position
    let mut e = s.clone();
    for n in &mut e.nodes {
        n.priority = 5;
    }
    assert_eq!(e.promotion_order(S1, &all), vec![nid(4), nid(7), nid(8)]);
}

#[test]
fn failover_promotion() {
    let s = rf4();
    let s = apply_ok(
        &s,
        &[Transition::Health {
            node: nid(1),
            fail: true,
        }],
    );
    let chosen = s.choose_primary(S1, &all, &[]).unwrap();
    assert_eq!(chosen, nid(8));
    // node 7 is suspected (PFAIL) by the lease detector: left out of the new ISR
    let flagged = |n: &kv_ctrl::NodeId| *n == nid(7);
    let t = s
        .failover_promote(S1, chosen, &flagged, vec![9; 64])
        .unwrap();
    let Transition::Promote {
        primary,
        term,
        ref isr,
        ref term_end,
        ..
    } = t
    else {
        panic!()
    };
    assert_eq!((primary, term), (nid(8), 2));
    assert_eq!(isr, &vec![nid(4), nid(8)]);
    assert_eq!(
        term_end,
        &TermEnd {
            term: 1,
            takeover: vec![9; 64]
        }
    );
    let n = apply_ok(&s, std::slice::from_ref(&t));
    let sh = n.shard(S1).unwrap();
    assert_eq!(
        (sh.primary, sh.term, sh.status),
        (Some(nid(8)), 2, ShardStatus::Serving)
    );
    assert_eq!(
        sh.history,
        vec![TermEnd {
            term: 1,
            takeover: vec![9; 64]
        }]
    );
    let st = |i| *sh.member(&nid(i)).unwrap();
    assert_eq!(
        (st(1).state, st(1).in_isr),
        (MemberState::Failed, false),
        "old primary stays a member"
    );
    assert_eq!(
        (st(4).state, st(4).in_isr),
        (MemberState::Reconciling, true),
        "repaired before advertised"
    );
    assert_eq!((st(7).state, st(7).in_isr), (MemberState::Failed, false));
    assert_eq!((st(8).state, st(8).in_isr), (MemberState::InSync, true));
    assert_eq!(
        sh.members.len(),
        4,
        "promotion never changes the member list"
    );
    assert_eq!(
        redirect::for_slot(&n, 0, &none),
        Redirect::Moved(0, n.node(&nid(8)).unwrap().client)
    );
    let a = n.assignment_for(&nid(8), 1, &[]).role.unwrap();
    assert!(a.is_primary && a.term == 2 && a.history.len() == 1);
    // the same promotion again is stale; skipping a term is refused
    assert_eq!(n.apply(&[t]).err(), Some(Reject::StaleTerm));
    let skip = Transition::Promote {
        shard: S1,
        primary: nid(4),
        term: 4,
        isr: vec![nid(4)],
        term_end: TermEnd {
            term: 2,
            takeover: takeover(),
        },
    };
    assert!(is_pre(n.apply(&[skip])));
    // the new primary must come from the committed ISR, and the ISR may only shrink
    let bad = Transition::Promote {
        shard: S1,
        primary: nid(7),
        term: 3,
        isr: vec![nid(7)],
        term_end: TermEnd {
            term: 2,
            takeover: takeover(),
        },
    };
    assert!(is_pre(n.apply(&[bad])));
    let grow = Transition::Promote {
        shard: S1,
        primary: nid(4),
        term: 3,
        isr: vec![nid(4), nid(1)],
        term_end: TermEnd {
            term: 2,
            takeover: takeover(),
        },
    };
    assert!(is_pre(n.apply(&[grow])));
    let short = Transition::Promote {
        shard: S1,
        primary: nid(4),
        term: 3,
        isr: vec![nid(4)],
        term_end: TermEnd {
            term: 2,
            takeover: vec![0; 3],
        },
    };
    assert!(is_pre(n.apply(&[short])), "one takeover seq per partition");
}

#[test]
fn history_keeps_last_four() {
    let mut s = rf4();
    for _ in 0..6 {
        let p = s.shard(S1).unwrap().primary.unwrap();
        let next = s.handoff_order(S1, &all).into_iter().next().unwrap();
        assert_ne!(p, next);
        s = apply_ok(&s, &[s.handoff_promote(S1, next, takeover()).unwrap()]);
    }
    let sh = s.shard(S1).unwrap();
    assert_eq!(sh.term, 7);
    assert_eq!(
        sh.history.iter().map(|h| h.term).collect::<Vec<_>>(),
        vec![3, 4, 5, 6]
    );
}

#[test]
fn graceful_handoff_keeps_old_primary_in_sync() {
    let s = three_by_two();
    let t = s.handoff_promote(S1, nid(4), vec![5; 64]).unwrap();
    let n = apply_ok(&s, &[t]);
    let sh = n.shard(S1).unwrap();
    assert_eq!(sh.primary, Some(nid(4)));
    assert_eq!(
        *sh.member(&nid(1)).unwrap(),
        Member {
            node: nid(1),
            state: MemberState::InSync,
            in_isr: true
        }
    );
    assert!(
        s.handoff_promote(S1, nid(2), takeover()).is_err(),
        "target must be in the ISR"
    );
}

#[test]
fn rf1_re_promotion() {
    let mut s = ClusterState::initial(ClusterParams::default(), [3; 16]);
    s = apply_ok(
        &s,
        &[
            node_add(1, 0),
            admin(AdminOp::ShardCreate {
                id: S1,
                primary: nid(1),
                min_isr: 1,
            }),
            admin(AdminOp::SlotsAssign {
                slots: ranges(0, 16383),
                shard: S1,
            }),
        ],
    );
    let n = apply_ok(
        &s,
        &[Transition::Health {
            node: nid(1),
            fail: true,
        }],
    );
    assert_eq!(n.shard(S1).unwrap().status, ShardStatus::NoPrimary);
    assert!(n.choose_primary(S1, &all, &[]).is_none());
    let t = n.failover_promote(S1, nid(1), &none, takeover()).unwrap();
    assert!(
        is_pre(n.apply(std::slice::from_ref(&t))),
        "the returning primary must have its FAIL flag cleared first"
    );
    let n = apply_ok(
        &n,
        &[
            Transition::Health {
                node: nid(1),
                fail: false,
            },
            t,
        ],
    );
    let sh = n.shard(S1).unwrap();
    assert_eq!(
        (sh.primary, sh.term, sh.status),
        (Some(nid(1)), 2, ShardStatus::Serving)
    );
}

#[test]
fn isr_changes() {
    let s = three_by_two();
    let n = apply_ok(
        &s,
        &[Transition::Isr {
            shard: S1,
            term: 1,
            node: nid(4),
            add: false,
        }],
    );
    let m = *n.shard(S1).unwrap().member(&nid(4)).unwrap();
    assert_eq!((m.state, m.in_isr), (MemberState::CatchingUp, false));
    assert_eq!(
        n.apply(&[Transition::Isr {
            shard: S1,
            term: 0,
            node: nid(4),
            add: true
        }])
        .err(),
        Some(Reject::StaleTerm)
    );
    assert!(is_pre(n.apply(&[Transition::Isr {
        shard: S1,
        term: 2,
        node: nid(4),
        add: true
    }])));
    assert!(
        is_pre(n.apply(&[Transition::Isr {
            shard: S1,
            term: 1,
            node: nid(1),
            add: false
        }])),
        "primary"
    );
    assert!(
        is_pre(n.apply(&[Transition::Isr {
            shard: S1,
            term: 1,
            node: nid(2),
            add: true
        }])),
        "not a member"
    );
    let n = apply_ok(
        &n,
        &[Transition::Isr {
            shard: S1,
            term: 1,
            node: nid(4),
            add: true,
        }],
    );
    assert!(n.shard(S1).unwrap().member(&nid(4)).unwrap().in_isr);
    assert!(is_pre(n.apply(&[Transition::MemberState {
        shard: S1,
        node: nid(1),
        state: MemberState::Failed
    }])));
}

fn migrate(id: u64, a: u16, b: u16, dst: ShardId) -> Transition {
    admin(AdminOp::MigrateStart {
        id,
        slots: ranges(a, b),
        dst,
        rate_mbps: Some(100),
        barriers: 2,
    })
}

fn phase(id: u64, phase: MigPhase) -> Transition {
    Transition::Migration {
        id,
        phase,
        slots_to_dst: matches!(phase, MigPhase::Cutover | MigPhase::Done),
    }
}

#[test]
fn migration_lifecycle() {
    let s = three_by_two();
    assert!(
        is_pre(s.apply(&[migrate(1, 5000, 6000, S3)])),
        "two source shards"
    );
    assert!(
        is_pre(s.apply(&[migrate(1, 0, 10, S1)])),
        "already at the destination"
    );
    assert!(is_pre(s.apply(&[admin(AdminOp::MigrateStart {
        id: 1,
        slots: ranges(0, 10),
        dst: S2,
        rate_mbps: Some(0),
        barriers: 2
    })])));
    let n = apply_ok(&s, &[migrate(1, 0, 99, S2)]);
    assert!(
        is_pre(n.apply(&[migrate(2, 50, 60, S3)])),
        "overlaps an unfinished migration"
    );
    assert!(is_pre(n.apply(&[migrate(1, 200, 300, S3)])), "id reused");
    // tasks: Out on the source primary, In (with the source's repl address) on the destination
    let out = n.assignment_for(&nid(1), 1, &[]).role.unwrap().migrations;
    assert_eq!(
        out,
        vec![MigrationTask::Out {
            id: 1,
            slots: ranges(0, 99),
            phase: MigPhase::Copy
        }]
    );
    let inn = n.assignment_for(&nid(2), 1, &[]).role.unwrap().migrations;
    let a_repl = PeerAddr(n.node(&nid(1)).unwrap().repl);
    assert_eq!(
        inn,
        vec![MigrationTask::In {
            id: 1,
            slots: ranges(0, 99),
            src_repl: a_repl,
            phase: MigPhase::Copy
        }]
    );
    assert!(
        n.assignment_for(&nid(4), 1, &[])
            .role
            .unwrap()
            .migrations
            .is_empty()
    );
    // phases
    assert!(
        is_pre(n.apply(&[phase(1, MigPhase::Done)])),
        "done only after cutover"
    );
    assert!(is_pre(n.apply(&[Transition::Migration {
        id: 1,
        phase: MigPhase::Cutover,
        slots_to_dst: false
    }])));
    assert_eq!(
        n.apply(&[phase(9, MigPhase::Ready)]).err(),
        Some(Reject::UnknownMigration)
    );
    let n = apply_ok(&n, &[phase(1, MigPhase::Ready)]);
    assert_eq!(n.slot_owner(0), Some(S1));
    let n = apply_ok(&n, &[phase(1, MigPhase::Cutover)]);
    assert_eq!(
        n.slot_owner(0),
        Some(S2),
        "the coordinator redirects to B from the cutover commit on"
    );
    assert_eq!(n.slot_owner(100), Some(S1));
    assert_eq!(
        redirect::for_slot(&n, 5, &none),
        Redirect::Moved(5, n.node(&nid(2)).unwrap().client)
    );
    let n2 = apply_ok(&n, &[phase(1, MigPhase::Done)]);
    assert_eq!(n2.migrations[0].phase, MigPhase::Done);
    assert_eq!(n2.slot_owner(0), Some(S2));
    assert_eq!(
        apply_ok(&n2, &[phase(1, MigPhase::Done)]).migrations,
        n2.migrations,
        "repeated report"
    );
    assert!(is_pre(n2.apply(&[admin(AdminOp::MigrateAbort { id: 1 })])));
    // a failover of either side before Done reverts routing and pauses
    let p = n.failover_promote(S2, nid(5), &none, takeover()).unwrap();
    let f = apply_ok(
        &n,
        &[Transition::Health {
            node: nid(2),
            fail: true,
        }],
    );
    assert_eq!(f.migrations[0].phase, MigPhase::Paused);
    assert_eq!(f.slot_owner(0), Some(S1), "routing back to A");
    let f = apply_ok(&f, &[p]);
    assert_eq!(
        f.migrations[0].phase,
        MigPhase::Paused,
        "nothing resumes automatically"
    );
    assert!(is_pre(f.apply(&[phase(1, MigPhase::Cutover)])));
    let r = apply_ok(&f, &[admin(AdminOp::MigrateResume { id: 1 })]);
    assert_eq!(r.migrations[0].phase, MigPhase::Copy);
    let a = apply_ok(&r, &[admin(AdminOp::MigrateAbort { id: 1 })]);
    assert!(a.migrations.is_empty() && a.slot_owner(0) == Some(S1));
    // pause / abort during cutover revert routing too
    let pz = apply_ok(&n, &[admin(AdminOp::MigratePause { id: 1 })]);
    assert_eq!(
        (pz.migrations[0].phase, pz.slot_owner(0)),
        (MigPhase::Paused, Some(S1))
    );
    let ab = apply_ok(&n, &[admin(AdminOp::MigrateAbort { id: 1 })]);
    assert_eq!(ab.slot_owner(0), Some(S1));
    assert!(is_pre(ab.apply(&[admin(AdminOp::ShardDelete { id: S1 })])));
}

#[test]
fn done_migrations_are_bounded() {
    let mut s = three_by_two();
    for id in 0..(MAX_DONE_MIGRATIONS as u64 + 10) {
        let (src, dst) = if id % 2 == 0 { (S1, S2) } else { (S2, S1) };
        let _ = src;
        s = apply_ok(&s, &[migrate(id, 0, 0, dst)]);
        s = apply_ok(
            &s,
            &[phase(id, MigPhase::Cutover), phase(id, MigPhase::Done)],
        );
        assert!(s.migrations.len() <= MAX_DONE_MIGRATIONS);
    }
    assert_eq!(
        s.migrations.last().unwrap().id,
        MAX_DONE_MIGRATIONS as u64 + 9
    );
    // deleting a shard drops the finished records that name it
    let s = apply_ok(
        &s,
        &[
            node_add(9, 0),
            admin(AdminOp::ShardCreate {
                id: ShardId(9),
                primary: nid(9),
                min_isr: 1,
            }),
        ],
    );
    let s = apply_ok(
        &s,
        &[
            migrate(1000, 1, 1, ShardId(9)),
            phase(1000, MigPhase::Cutover),
            phase(1000, MigPhase::Done),
        ],
    );
    let s = apply_ok(
        &s,
        &[
            migrate(1001, 1, 1, S1),
            phase(1001, MigPhase::Cutover),
            phase(1001, MigPhase::Done),
        ],
    );
    let s = apply_ok(&s, &[admin(AdminOp::ShardDelete { id: ShardId(9) })]);
    assert!(
        s.migrations
            .iter()
            .all(|m| m.src != ShardId(9) && m.dst != ShardId(9))
    );
}

#[test]
fn encode_decode() {
    let s = rf4();
    let s = apply_ok(
        &s,
        &[
            migrate(4, 100, 200, S3),
            Transition::Health {
                node: nid(6),
                fail: true,
            },
        ],
    );
    let b = s.encode();
    let d = ClusterState::decode(&b).unwrap();
    assert!(d == s);
    assert_eq!(d.encode(), b, "canonical");
    let (n, nb) = s.apply_with_encoding(&[]).unwrap();
    assert_eq!(n.encode(), nb);
    // run-length slots keep the encoding small
    assert!(b.len() < 4096, "{}", b.len());
    let mut bad = b.clone();
    bad[0] = STATE_FORMAT_VERSION + 1;
    assert_eq!(ClusterState::decode(&bad).err(), Some(Reject::Malformed));
    let mut bad = b.clone();
    bad.push(0);
    assert_eq!(ClusterState::decode(&bad).err(), Some(Reject::Malformed));
    assert_eq!(ClusterState::decode(&[]).err(), Some(Reject::Malformed));
    assert_eq!(
        ClusterState::decode(&b[..b.len() / 2]).err(),
        Some(Reject::Malformed)
    );
    // structurally invalid content is refused
    let mut broken = s.clone();
    broken.slots[0] = 4242;
    assert_eq!(
        ClusterState::decode(&broken.encode()).err(),
        Some(Reject::Malformed)
    );
    let mut broken = s.clone();
    let dup = broken.shards[0].members[0];
    broken.shards[1].members.push(dup);
    assert_eq!(
        ClusterState::decode(&broken.encode()).err(),
        Some(Reject::Malformed)
    );
}

#[test]
fn state_too_large() {
    // 1700 shards, each with 4 history entries of 128 maximal seqs: ~8.7 MB encoded.
    let params = ClusterParams {
        partitions: 128,
        ..ClusterParams::default()
    };
    let mut s = ClusterState::initial(params, [0; 16]);
    for i in 0..1700u32 {
        let id = {
            let mut b = [0u8; 20];
            b[..4].copy_from_slice(&i.to_le_bytes());
            kv_ctrl::NodeId(b)
        };
        let ip = std::net::Ipv4Addr::from(0x0A00_0000 + i);
        let at = |port| SocketAddr::new(ip.into(), port);
        s.nodes.push(NodeRecord {
            id,
            ctrl: at(1),
            client: at(2),
            repl: at(3),
            zone: String::new(),
            priority: 0,
            flags: NodeFlags::default(),
            partitions: 128,
            added_version: 1,
        });
        s.shards.push(ShardRecord {
            id: ShardId(i as u16),
            primary: Some(id),
            members: vec![Member {
                node: id,
                state: MemberState::InSync,
                in_isr: true,
            }],
            term: 5,
            min_isr: 1,
            history: (1..=4)
                .map(|t| TermEnd {
                    term: t,
                    takeover: vec![u64::MAX; 128],
                })
                .collect(),
            status: ShardStatus::Serving,
        });
    }
    assert!(s.encode().len() > MAX_STATE_BYTES);
    assert_eq!(s.apply(&[]).err(), Some(Reject::StateTooLarge));
}
