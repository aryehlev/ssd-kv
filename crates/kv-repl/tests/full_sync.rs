//! LC-39 `full_sync_keeps_layout` (DESIGN §23.7, §25.2): a merge full sync of a prefilled
//! member with 1% divergent keys confirms identical records without writing them, rewrites only
//! the divergent ones through the normal write path and deletes the member's extra keys with
//! ordinary deletes: at least 98% of the member's index entries keep their page, its AG set is
//! unchanged, and its page writes stay within 1.5x the number of divergent keys.
//!
//! The records travel the real replication path: the primary's images are encoded as
//! `SNAP_BEGIN/SNAP_REC/SNAP_END/SYNC_DONE` frames, decoded by a `FrameDecoder` and turned into
//! actions by the member's `MemberStream`, which the member applies with
//! `ApplyMode::SnapshotMerge`.

use kv_common::{Access, Now};
use kv_index::VType;
use kv_repl::Seq;
use kv_repl::backlog::Backlog;
use kv_repl::frame::{AcceptMode, Frame, FrameDecoder, SnapRec};
use kv_repl::member::{MemberAction, MemberStream};
use kv_storage::effect::Op;
use kv_storage::engine::{EngineCfg, LocalEngine, format_devices};
use kv_storage::io::{BlockDevice, DevInfo, IoDone, IoOp, IoReq, PageUpdateMode};
use kv_storage::keyspace::{ApplyMode, ApplyOutcome};
use kv_storage::partition::{Origin, ReqKind, Request};

/// A RAM device whose requests complete at the next poll (write-through).
struct Ram {
    data: Vec<u8>,
    pending: Vec<IoReq>,
}

impl Ram {
    fn new(pages: u64) -> Self {
        Ram {
            data: vec![0u8; pages as usize * 4096],
            pending: Vec::new(),
        }
    }
}

impl BlockDevice for Ram {
    fn info(&self) -> DevInfo {
        DevInfo {
            pages: (self.data.len() / 4096) as u64,
            lba: 4096,
            write_through: true,
            discard: false,
            page_update: PageUpdateMode::TwoImage,
            atomic_unit: None,
        }
    }

    fn submit(&mut self, r: IoReq) {
        self.pending.push(r);
    }

    fn poll(&mut self, out: &mut Vec<IoDone>, _wait: bool) {
        for mut r in self.pending.drain(..) {
            let res = match r.check(4096, self.data.len() as u64) {
                Err(e) => Err(e),
                Ok(()) => {
                    let (off, len) = (r.offset as usize, r.len as usize);
                    match &mut r.op {
                        IoOp::Read { buf } => {
                            buf.as_mut_slice()[..len].copy_from_slice(&self.data[off..off + len]);
                            Ok(r.len)
                        }
                        IoOp::Write { buf, buf_off, .. } => {
                            let b = *buf_off as usize;
                            self.data[off..off + len].copy_from_slice(&buf.as_slice()[b..b + len]);
                            Ok(r.len)
                        }
                        IoOp::Flush => Ok(0),
                        IoOp::Discard => Ok(r.len),
                    }
                }
            };
            let tag = r.tag;
            out.push(IoDone {
                tag,
                res,
                buf: r.into_buf(),
            });
        }
    }
}

const T0: u64 = 1_800_000_000_000;

fn engine(seed: u64) -> LocalEngine<Ram> {
    let mut d = Ram::new(48 * 4096);
    format_devices(&mut [&mut d as &mut dyn BlockDevice], 16, true, 0, seed).unwrap();
    let mut cfg = EngineCfg::defaults(Vec::new());
    cfg.cache_bytes = 16 << 20;
    let mut e = LocalEngine::open(vec![d], &cfg).unwrap();
    e.set_now(Now {
        mono_ns: T0 * 1_000_000,
        boot_ns: T0 * 1_000_000,
        unix_ms: T0,
    });
    e.recover().unwrap();
    e
}

fn req(key: &[u8], access: Access, origin: Origin) -> Request {
    Request {
        db: 0,
        keys: [(Box::<[u8]>::from(key), access)].into_iter().collect(),
        kind: ReqKind::Keyed,
        origin,
    }
}

fn put(e: &mut LocalEngine<Ram>, key: &[u8], value: &[u8]) {
    let p = e.partition_of(key);
    e.run(p, req(key, Access::Write, Origin::Client), &mut |ks| {
        ks.put(key, value, VType::String, None)
    })
    .unwrap();
}

fn get(e: &mut LocalEngine<Ram>, key: &[u8]) -> Option<Vec<u8>> {
    let p = e.partition_of(key);
    e.run(p, req(key, Access::Read, Origin::Client), &mut |ks| {
        Ok(ks.get(key)?.map(|(_, v)| v.to_vec()))
    })
    .unwrap()
    .0
}

fn page_writes(e: &LocalEngine<Ram>) -> u64 {
    (0..e.partitions())
        .map(|p| {
            let c = e.partition(p).counters();
            c.page_writes_update
                + c.page_writes_insert
                + c.page_writes_delete
                + c.page_writes_move
                + c.page_writes_txn
                + c.extent_writes
        })
        .sum()
}

fn value(i: usize, variant: u8) -> Vec<u8> {
    let mut v = format!("value-{i}-").into_bytes();
    v.extend(std::iter::repeat_n(b'a' + variant, 40 + i % 60));
    v
}

#[test]
fn full_sync_keeps_layout() {
    const N: usize = 3000;
    let mut primary = engine(11);
    let mut member = engine(22);
    let keys: Vec<Vec<u8>> = (0..N).map(|i| format!("key:{i}").into_bytes()).collect();
    // 1% of the keys differ; the member also holds keys the primary does not
    let divergent: Vec<usize> = (0..N).filter(|i| i % 100 == 7).collect();
    for (i, k) in keys.iter().enumerate() {
        put(&mut primary, k, &value(i, 0));
        put(
            &mut member,
            k,
            &value(i, if divergent.contains(&i) { 1 } else { 0 }),
        );
    }
    let extra: Vec<Vec<u8>> = (0..N / 100)
        .map(|i| format!("extra:{i}").into_bytes())
        .collect();
    for (i, k) in extra.iter().enumerate() {
        put(&mut member, k, &value(i, 2));
    }
    assert!(primary.settle(100_000) && member.settle(100_000));
    let pages_before: Vec<_> = keys
        .iter()
        .map(|k| member.locate(0, k).map(|l| l.page))
        .collect();
    let ags_before = member.meta().ag_usage();
    let writes_before = page_writes(&member);

    // the primary's walk, as frames (a single stream for the test; per partition in kvd)
    let at = Seq::new(2, 1);
    let mut wire = Vec::new();
    kv_repl::frame::encode(
        &Frame::Accept {
            term: 2,
            mode: AcceptMode::Full { at },
        },
        0,
        2,
        &mut wire,
    );
    kv_repl::frame::encode(&Frame::SnapBegin { slot: 0 }, 0, 2, &mut wire);
    for k in &keys {
        let v = get(&mut primary, k).expect("primary holds the key");
        kv_repl::frame::encode(
            &Frame::SnapRec(SnapRec {
                db: 0,
                vtype: 1,
                expire_at: 0,
                key: k.clone().into(),
                value: v,
            }),
            0,
            2,
            &mut wire,
        );
    }
    kv_repl::frame::encode(
        &Frame::SnapEnd {
            slot: 0,
            count: N as u64,
        },
        0,
        2,
        &mut wire,
    );
    kv_repl::frame::encode(&Frame::SyncDone { at }, 0, 2, &mut wire);

    // the member: decode, act, apply through the normal write path
    let mut stream = MemberStream::with_backlog(0, Backlog::new(1 << 16));
    let mut dec = FrameDecoder::new();
    let mut off = 0;
    let mut confirmed = hashbrown::HashSet::new();
    let (mut written, mut began, mut done) = (0usize, false, None);
    while off < wire.len() {
        let (_, _, f, n) = dec.feed(&wire[off..]).unwrap().expect("whole frames");
        off += n;
        let mut acts = vec![stream.on_frame(f).unwrap()];
        while let Some(a) = stream.next_action() {
            acts.push(a);
        }
        for a in acts {
            match a {
                MemberAction::SyncBegin(s) => {
                    assert_eq!(s, at);
                    began = true;
                }
                MemberAction::Snap(rec, stamp) => {
                    let p = member.partition_of(&rec.key);
                    let key = rec.key.clone();
                    let op = Op::Put {
                        db: rec.db,
                        vtype: rec.vtype,
                        expire_at: rec.expire_at,
                        key: rec.key,
                        value: rec.value,
                    };
                    let (o, _) = member
                        .run(
                            p,
                            req(&key, Access::ReadWrite, Origin::Snapshot(stamp)),
                            &mut |ks| ks.apply_replicated(&op, stamp, ApplyMode::SnapshotMerge),
                        )
                        .unwrap();
                    match o {
                        ApplyOutcome::Confirmed => {
                            confirmed.insert(key.to_vec());
                        }
                        ApplyOutcome::Applied => written += 1,
                        ApplyOutcome::Skipped => panic!("a merge never skips"),
                    }
                }
                MemberAction::SyncDone(s) => done = Some(s),
                _ => {}
            }
        }
    }
    assert!(began && done == Some(at));
    assert_eq!(
        written,
        divergent.len(),
        "only the divergent keys are written"
    );
    assert_eq!(
        confirmed.len(),
        N - divergent.len(),
        "identical records are confirmed in place"
    );
    // the prune: keys of the member that the walk did not confirm and that are older than `at`
    for k in extra.iter() {
        let p = member.partition_of(k);
        member
            .run(p, req(k, Access::Write, Origin::Snapshot(at)), &mut |ks| {
                if ks.stored_seq(k)? < at {
                    ks.apply_replicated(
                        &Op::Del {
                            db: 0,
                            key: k.clone().into(),
                        },
                        at,
                        ApplyMode::Image,
                    )?;
                }
                Ok(())
            })
            .unwrap();
    }
    assert!(member.settle(100_000));

    // the member now equals the primary
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(get(&mut member, k), Some(value(i, 0)), "key {i}");
    }
    for k in &extra {
        assert_eq!(
            get(&mut member, k),
            None,
            "the member's extra keys are deleted"
        );
    }
    // layout: entries keep their pages, the AG set is unchanged, writes stay proportional
    let kept = keys
        .iter()
        .zip(&pages_before)
        .filter(|(k, before)| member.locate(0, k).map(|l| l.page) == **before)
        .count();
    assert!(
        kept * 100 >= N * 98,
        "only {kept} of {N} index entries kept their page"
    );
    assert_eq!(
        member.meta().ag_usage(),
        ags_before,
        "no AG is acquired or released by the merge"
    );
    let writes = page_writes(&member) - writes_before;
    let bound = (divergent.len() + extra.len()) * 3 / 2;
    assert!(
        writes as usize <= bound.max(1),
        "{writes} page writes for {} divergent and {} extra keys",
        divergent.len(),
        extra.len()
    );
}
