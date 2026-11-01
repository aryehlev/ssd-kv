//! Code-verifiable control-plane invariants of kv-ctrl (DESIGN §23.4, LC-26, LC-30) and codec
//! properties (§25.2).
//!
//! The field-name checks read the names straight from the serde derive (a probing
//! `Deserializer` asks the derived impl for each variant's field list), so they hold for the
//! types as compiled, not just for the source text the lint greps.

use std::cell::RefCell;
use std::net::SocketAddr;

use kv_common::SlotSet;
use kv_ctrl::{
    CodecError, CoordEndpoint, CoordId, CoordToNode, Health, MemberState, MigPhase, MigrationTask,
    NodeAssignment, NodeError, NodeId, NodeParams, NodeToCoord, PeerAddr, ShardId, ShardRole,
    SlotRanges, TermEnd, decode, encode,
};
use proptest::prelude::*;
use serde::de::{self, DeserializeSeed, EnumAccess, IntoDeserializer, VariantAccess, Visitor};
use serde::{Deserialize, Deserializer};

type DeError = de::value::Error;

#[derive(Default)]
struct Seen {
    variants: Vec<&'static str>,
    current: Option<&'static str>,
    fields: Vec<&'static str>,
}

/// Deserializer that selects variant `idx` of an enum (or reads a struct's field list) and
/// records the names the derived impl passes in, then stops with an error.
struct Probe<'a> {
    idx: u32,
    seen: &'a RefCell<Seen>,
}

impl<'de> Deserializer<'de> for Probe<'_> {
    type Error = DeError;

    fn deserialize_any<V: Visitor<'de>>(self, _v: V) -> Result<V::Value, DeError> {
        Err(de::Error::custom("probe: not an enum"))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        _v: V,
    ) -> Result<V::Value, DeError> {
        self.seen.borrow_mut().fields = fields.to_vec();
        Err(de::Error::custom("probe: struct recorded"))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DeError> {
        {
            let mut s = self.seen.borrow_mut();
            s.variants = variants.to_vec();
            s.current = variants.get(self.idx as usize).copied();
            s.fields.clear();
        }
        if self.idx as usize >= variants.len() {
            return Err(de::Error::custom("probe: no such variant"));
        }
        visitor.visit_enum(ProbeEnum {
            idx: self.idx,
            seen: self.seen,
        })
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option
        unit unit_struct newtype_struct seq tuple tuple_struct map identifier ignored_any
    }
}

struct ProbeEnum<'a> {
    idx: u32,
    seen: &'a RefCell<Seen>,
}

impl<'de, 'a> EnumAccess<'de> for ProbeEnum<'a> {
    type Error = DeError;
    type Variant = ProbeVariant<'a>;

    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, ProbeVariant<'a>), DeError> {
        let v = seed.deserialize(IntoDeserializer::<DeError>::into_deserializer(self.idx))?;
        Ok((v, ProbeVariant { seen: self.seen }))
    }
}

struct ProbeVariant<'a> {
    seen: &'a RefCell<Seen>,
}

impl<'de> VariantAccess<'de> for ProbeVariant<'_> {
    type Error = DeError;

    fn unit_variant(self) -> Result<(), DeError> {
        Err(de::Error::custom("probe: unit variant"))
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, _seed: T) -> Result<T::Value, DeError> {
        Err(de::Error::custom("probe: newtype variant"))
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, _v: V) -> Result<V::Value, DeError> {
        Err(de::Error::custom("probe: tuple variant"))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        _v: V,
    ) -> Result<V::Value, DeError> {
        self.seen.borrow_mut().fields = fields.to_vec();
        Err(de::Error::custom("probe: struct variant recorded"))
    }
}

/// `(variant name, field names)` of every variant of `T`, in declaration order.
fn enum_fields<T: for<'de> Deserialize<'de>>() -> Vec<(&'static str, Vec<&'static str>)> {
    let seen = RefCell::new(Seen::default());
    let mut out = Vec::new();
    for idx in 0.. {
        let r = T::deserialize(Probe { idx, seen: &seen });
        assert!(r.is_err(), "the probe never produces a value");
        let s = seen.borrow();
        let Some(name) = s.current else { break };
        out.push((name, s.fields.clone()));
        if idx as usize + 1 >= s.variants.len() {
            break;
        }
    }
    out
}

fn struct_fields<T: for<'de> Deserialize<'de>>() -> Vec<&'static str> {
    let seen = RefCell::new(Seen::default());
    assert!(
        T::deserialize(Probe {
            idx: 0,
            seen: &seen
        })
        .is_err()
    );
    seen.into_inner().fields
}

const TOPOLOGY_WORDS: &[&str] = &[
    "role", "shard", "slot", "owned", "key", "term", "primary", "member",
];

/// LC-26: the replies to coordinator requests carry identity, acknowledgements and positions
/// only; no role, shard, slot, key-count or term field.
#[test]
fn reply_variants_carry_no_topology_fields() {
    let all = enum_fields::<NodeToCoord>();
    let names: Vec<_> = all.iter().map(|(n, _)| *n).collect();
    assert_eq!(
        names,
        [
            "HelloAck",
            "AssignAck",
            "FenceAck",
            "HandoffDone",
            "Error",
            "Renew",
            "IsrShrink",
            "IsrExpand",
            "MemberReady",
            "MigrationReady",
            "SealDone"
        ]
    );
    let replies = ["HelloAck", "AssignAck", "FenceAck", "HandoffDone", "Error"];
    for (name, fields) in &all {
        if !replies.contains(name) {
            continue;
        }
        for f in fields {
            for w in TOPOLOGY_WORDS {
                assert!(!f.contains(w), "reply {name} has field {f}");
            }
        }
    }
    assert_eq!(
        all[0].1,
        ["node", "incarnation", "set_uuid", "partitions", "ready"],
        "HelloAck is identity and readiness only"
    );
    // NodeError has no contents: unit variants only.
    for (name, fields) in enum_fields::<NodeError>() {
        assert!(fields.is_empty(), "NodeError::{name} has contents");
    }
    // The reply classification used by the coordinator agrees with the declaration.
    let samples = [
        NodeToCoord::HelloAck {
            node: None,
            incarnation: 1,
            set_uuid: [0; 16],
            partitions: 64,
            ready: true,
        },
        NodeToCoord::AssignAck { version: 1 },
        NodeToCoord::FenceAck {
            req: 1,
            applied: vec![],
        },
        NodeToCoord::HandoffDone {
            req: 1,
            last: vec![],
        },
        NodeToCoord::Error(NodeError::StaleAssignment),
        NodeToCoord::SealDone { id: 1 },
    ];
    for s in &samples {
        assert_eq!(
            s.is_reply(),
            replies.contains(&kind_to_variant(s.kind())),
            "{}",
            s.kind()
        );
    }
}

fn kind_to_variant(kind: &str) -> &'static str {
    match kind {
        "hello-ack" => "HelloAck",
        "assign-ack" => "AssignAck",
        "fence-ack" => "FenceAck",
        "handoff-done" => "HandoffDone",
        "error" => "Error",
        _ => "other",
    }
}

/// LC-26: no two-phase or ballot message types on the control channel.
#[test]
fn no_two_phase_message_types() {
    let banned = ["prepare", "precommit", "abort", "vote", "ballot", "commit"];
    for (name, _) in enum_fields::<CoordToNode>()
        .into_iter()
        .chain(enum_fields::<NodeToCoord>())
    {
        let lower = name.to_ascii_lowercase();
        for b in banned {
            assert!(!lower.contains(b), "message type {name}");
        }
    }
    let coord: Vec<_> = enum_fields::<CoordToNode>()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(
        coord,
        [
            "Hello",
            "Assign",
            "LeaseGrant",
            "Fence",
            "Handoff",
            "MigrationPhase",
            "Ping"
        ]
    );
}

/// LC-30: `Health` has no storage-maintenance field.
#[test]
fn health_has_no_maintenance_fields() {
    let fields = struct_fields::<Health>();
    assert_eq!(
        fields,
        ["ready", "failed", "read_only_partitions", "corrupt_pages"]
    );
    for f in fields {
        for w in [
            "scrub", "compact", "defrag", "maint", "reclaim", "sweep", "warm",
        ] {
            assert!(!f.contains(w), "Health field {f}");
        }
    }
}

/// LC-26: an assignment carries the node's own role only; no full-map field.
#[test]
fn assignment_has_no_full_map_field() {
    assert_eq!(
        struct_fields::<NodeAssignment>(),
        [
            "version",
            "coord_term",
            "node_id",
            "coordinators",
            "role",
            "client_password",
            "params"
        ]
    );
    assert_eq!(
        struct_fields::<ShardRole>(),
        [
            "shard",
            "term",
            "is_primary",
            "owned_slots",
            "primary_repl",
            "members",
            "min_isr",
            "history",
            "migrations"
        ]
    );
}

fn addr(i: u16) -> SocketAddr {
    SocketAddr::from(([10, 0, (i >> 8) as u8, i as u8], 7000 + i))
}

fn sample_assignment(slots: &[u16], password: Option<String>) -> NodeAssignment {
    let mut owned = SlotSet::new();
    for &s in slots {
        owned.insert(s & 16383);
    }
    NodeAssignment {
        version: 12,
        coord_term: (1 << 32) + 5,
        node_id: NodeId([3; 20]),
        coordinators: vec![
            CoordEndpoint {
                id: CoordId([1; 20]),
                client: addr(1),
            },
            CoordEndpoint {
                id: CoordId([2; 20]),
                client: "[::1]:7000".parse().unwrap(),
            },
        ],
        role: Some(ShardRole {
            shard: ShardId(4),
            term: 9,
            is_primary: true,
            owned_slots: owned,
            primary_repl: Some(PeerAddr(addr(9))),
            members: vec![
                (NodeId([7; 20]), MemberState::Syncing, false),
                (NodeId([8; 20]), MemberState::InSync, true),
            ],
            min_isr: 2,
            history: vec![TermEnd {
                term: 8,
                takeover: vec![1, 2, 3],
            }],
            migrations: vec![
                MigrationTask::Out {
                    id: 1,
                    slots: SlotRanges(vec![(0, 10)]),
                    phase: MigPhase::Copy,
                },
                MigrationTask::In {
                    id: 2,
                    slots: SlotRanges(vec![(20, 20)]),
                    src_repl: PeerAddr(addr(5)),
                    phase: MigPhase::Paused,
                },
            ],
        }),
        client_password: password,
        params: NodeParams::default(),
    }
}

#[test]
fn assignment_round_trip() {
    let a = sample_assignment(&[0, 1, 2, 5000, 16383], Some("pw".into()));
    let mut out = Vec::new();
    encode(&CoordToNode::Assign(a.clone()), &mut out);
    let (m, n): (CoordToNode, usize) = decode(&out).unwrap().unwrap();
    assert_eq!(n, out.len());
    let CoordToNode::Assign(b) = m else {
        panic!("wrong variant")
    };
    assert!(a == b);
    assert!(a.same_projection(&b));
    let mut c = b.clone();
    c.version += 1;
    c.coord_term += 1;
    assert!(a.same_projection(&c) && a != c);
    assert!(c.order_key() > a.order_key());
    c.role.as_mut().unwrap().owned_slots.insert(77);
    assert!(!a.same_projection(&c));
    // Full bitmap stays far below the frame limit.
    let full: Vec<u16> = (0..16384).collect();
    let mut out = Vec::new();
    encode(
        &CoordToNode::Assign(sample_assignment(&full, None)),
        &mut out,
    );
    assert!(out.len() < 8 * 1024, "{}", out.len());
}

fn node_id() -> impl Strategy<Value = NodeId> {
    any::<[u8; 20]>().prop_map(NodeId)
}

fn node_msg() -> impl Strategy<Value = NodeToCoord> {
    prop_oneof![
        (
            proptest::option::of(node_id()),
            any::<u64>(),
            any::<[u8; 16]>(),
            any::<u16>(),
            any::<bool>()
        )
            .prop_map(|(node, incarnation, set_uuid, partitions, ready)| {
                NodeToCoord::HelloAck {
                    node,
                    incarnation,
                    set_uuid,
                    partitions,
                    ready,
                }
            }),
        any::<u64>().prop_map(|version| NodeToCoord::AssignAck { version }),
        (
            any::<u64>(),
            proptest::collection::vec(any::<u64>(), 0..130)
        )
            .prop_map(|(req, applied)| NodeToCoord::FenceAck { req, applied }),
        (
            any::<u64>(),
            proptest::collection::vec(any::<u64>(), 0..130)
        )
            .prop_map(|(req, last)| NodeToCoord::HandoffDone { req, last }),
        prop_oneof![
            Just(NodeError::StaleAssignment),
            Just(NodeError::BadAuth),
            Just(NodeError::Malformed)
        ]
        .prop_map(NodeToCoord::Error),
        (
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<(bool, bool, u16, u32)>()
        )
            .prop_map(|(seq, inc, v, h)| {
                NodeToCoord::Renew {
                    seq,
                    incarnation: inc,
                    version_seen: v,
                    health: Health {
                        ready: h.0,
                        failed: h.1,
                        read_only_partitions: h.2,
                        corrupt_pages: h.3,
                    },
                }
            }),
        (any::<u16>(), any::<u32>(), node_id()).prop_map(|(s, term, member)| {
            NodeToCoord::IsrShrink {
                shard: ShardId(s),
                term,
                member,
            }
        }),
        (any::<u16>(), any::<u32>(), node_id()).prop_map(|(s, term, member)| {
            NodeToCoord::IsrExpand {
                shard: ShardId(s),
                term,
                member,
            }
        }),
        (any::<u16>(), any::<u32>(), node_id()).prop_map(|(s, term, member)| {
            NodeToCoord::MemberReady {
                shard: ShardId(s),
                term,
                member,
            }
        }),
        any::<u64>().prop_map(|id| NodeToCoord::MigrationReady { id }),
        any::<u64>().prop_map(|id| NodeToCoord::SealDone { id }),
    ]
}

proptest! {
    /// Codec round trip: any sequence of node messages framed back to back decodes to the same
    /// messages, consuming exactly the bytes written, from any split point.
    #[test]
    fn prop_codec_round_trip(msgs in proptest::collection::vec(node_msg(), 1..8), cut in any::<prop::sample::Index>()) {
        let mut out = Vec::new();
        for m in &msgs {
            encode(m, &mut out);
        }
        let mut off = 0;
        for m in &msgs {
            let (d, n): (NodeToCoord, usize) = decode(&out[off..]).unwrap().unwrap();
            prop_assert_eq!(&d, m);
            off += n;
        }
        prop_assert_eq!(off, out.len());
        // a prefix of the first frame is incomplete, never an error
        let (_, first): (NodeToCoord, usize) = decode(&out).unwrap().unwrap();
        let c = cut.index(first);
        prop_assert!(decode::<NodeToCoord>(&out[..c]).unwrap().is_none());
    }

    /// Arbitrary bytes never panic the decoder; any error is one of the codec errors.
    #[test]
    fn prop_decode_garbage(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        match decode::<NodeToCoord>(&bytes) {
            Ok(Some((_, n))) => prop_assert!(n <= bytes.len()),
            Ok(None) => {}
            Err(CodecError::TooLarge | CodecError::Malformed | CodecError::UnsupportedVersion(_)) => {}
        }
        if let Ok(Some((_, n))) = decode::<CoordToNode>(&bytes) {
            prop_assert!(n <= bytes.len());
        }
    }

    /// SlotRanges <-> SlotSet round trip and parse/display round trip.
    #[test]
    fn prop_slot_ranges(slots in proptest::collection::btree_set(0u16..16384, 0..300)) {
        let mut set = SlotSet::new();
        for &s in &slots {
            set.insert(s);
        }
        let r = SlotRanges::from_slot_set(&set);
        prop_assert!(r.is_valid());
        prop_assert_eq!(r.normalized(), Some(r.clone()));
        prop_assert_eq!(r.slot_count(), slots.len());
        prop_assert!(r.to_slot_set() == set);
        prop_assert_eq!(r.iter_slots().collect::<Vec<_>>(), slots.iter().copied().collect::<Vec<_>>());
        if !r.is_empty() {
            prop_assert_eq!(SlotRanges::parse(&r.to_string()), Some(r.clone()));
        }
    }
}
