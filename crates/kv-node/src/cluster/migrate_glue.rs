//! Migration endpoints of the node's partitions (DESIGN §23.13).
//!
//! Every copy is a merge (§23.13 step 5): the destination may hold keys of these slots from an
//! earlier copy of the same migration (a stream that restarted, or `migrate resume`), and the
//! source may have deleted some of them meanwhile. The destination therefore records every
//! key the copy confirms (snapshot records and forwarded ops) and, at `SYNC_DONE { copy_end }`,
//! deletes the keys of the migrating slots that the copy did not confirm, before it
//! acknowledges `copy_end` (frames after it wait); so a key deleted at the source is never
//! served by the destination after the cutover.
//!
//! Source: the partition streams its migrating slots through the page-reference walk, paced by
//! static rates, forwards live groups of keys below the walk position, records its position
//! barriers and reports `MigrationReady` / seals after cutover. Destination: each partition
//! holding migrating slots dials the source with `Hello { migration }`, applies the copy as its
//! own writes and answers TRYAGAIN for those slots until the seal is committed. A partial copy
//! after a failure stays inert (not served, excluded by `slot_filter()`) until the operator
//! aborts or resumes.
//!
//! Source stream shape: `ACCEPT { Full { at } }`, per migrating slot `SNAP_BEGIN`, `SNAP_REC`s,
//! `SNAP_END` interleaved with the groups of passed keys, `SYNC_DONE { copy_end }`, then
//! `COMMIT { position }` markers (the source's last executed seq when the marker was written:
//! every earlier frame for these slots precedes it) and, after the cutover, `SEAL`. The
//! destination acknowledges a marker's position once every item before it executed as its own
//! write and its shard's commit point covers those writes; the source tests barriers against
//! those positions only (LC-36). The number of further barriers is the migration's
//! `--barriers k`, carried by the pushed `MigrationTask::Out`.

use std::net::SocketAddr;

use kv_common::Access;
use kv_common::pace::TokenBucket;
use kv_ctrl::{MigPhase, MigrationTask};
use std::collections::VecDeque;

use kv_repl::backlog::Backlog;
use kv_repl::codec::KeySet;
use kv_repl::frame::{AcceptMode, Frame, Hello, Recovered, RejectReason, SnapRec};
use kv_repl::member::{MemberAction, MemberStream};
use kv_repl::migrate::{MigrationDest, MigrationSource};
use kv_storage::effect::{Group, Op};
use kv_storage::keyspace::StoreError;
use kv_storage::layout::Seq;
use kv_storage::partition::{Origin, ReqKind, Request};
use smallvec::SmallVec;

use super::net::StreamIo;
use super::repl_glue::{
    ClusterWorker, KeyWalk, ReplJob, SEND_WINDOW, StreamRef, Then, WALK_KEYS, Walker,
};
use super::track::Markers;
use super::{AgentMsg, before, same_token};
use crate::runtime::backend::{Backend, ReplOut, ReplWork};
use crate::runtime::worker::Worker;

/// Pause before a destination redials its source.
const REDIAL_NS: u64 = 200_000_000;
/// A source repeats its position marker at least this often.
const POSITION_REPEAT_NS: u64 = 500_000_000;
/// A destination stream that delivered no frame for this long is closed and redialed.
const DEST_IDLE_NS: u64 = 5_000_000_000;

/// One migration endpoint of one partition.
pub enum MigrationEndpoint {
    Source(MigrationSource),
    Dest(MigrationDest),
}

/// The source side of one migration on one partition.
pub struct MigOut {
    pub id: u64,
    pub phase: MigPhase,
    slots: Vec<u16>,
    pub io: Option<StreamIo>,
    epoch: u64,
    src: MigrationSource,
    walk: KeyWalk,
    reading: bool,
    begun: bool,
    count: u64,
    bucket: TokenBucket,
    copy_end_sent: bool,
    pos_sent: Seq,
    sealed: bool,
    ready_reported: bool,
    rate: u64,
    /// Further barriers after `copy_end` (the migration's `--barriers k`).
    barriers: u8,
    /// When the last position marker was written (boot ns).
    pos_sent_ns: u64,
}

impl MigOut {
    fn new(id: u64, phase: MigPhase, slots: Vec<u16>, rate: u64, barriers: u8) -> Self {
        let rate = rate.max(1);
        MigOut {
            id,
            phase,
            src: MigrationSource::new(id, &slots, barriers),
            walk: KeyWalk::new(slots.clone()),
            slots,
            io: None,
            epoch: 0,
            reading: false,
            begun: false,
            count: 0,
            bucket: TokenBucket::new(rate, rate.max(256)),
            copy_end_sent: false,
            pos_sent: Seq::ZERO,
            sealed: false,
            ready_reported: false,
            rate,
            barriers,
            pos_sent_ns: 0,
        }
    }

    /// A new destination stream: the copy starts from the beginning.
    fn restart(&mut self, io: StreamIo) {
        if let Some(old) = self.io.replace(io) {
            old.abort();
        }
        self.epoch += 1;
        self.src = MigrationSource::new(self.id, &self.slots, self.barriers);
        self.walk = KeyWalk::new(self.slots.clone());
        self.reading = false;
        self.begun = false;
        self.count = 0;
        self.bucket = TokenBucket::new(self.rate, self.rate.max(256));
        self.copy_end_sent = false;
        self.pos_sent = Seq::ZERO;
        self.sealed = false;
        self.ready_reported = false;
    }

    pub fn abort(self) {
        if let Some(io) = &self.io {
            io.abort();
        }
    }

    pub fn closed(&mut self, stream: u64) {
        if self.io.as_ref().is_some_and(|i| i.id == stream) {
            self.io = None;
            self.epoch += 1;
        }
    }

    /// A group the source partition executed: forwarded when its slot migrates and the walk
    /// passed its key (or finished).
    pub fn on_group(
        &mut self,
        g: &Group,
        enc: &[u8],
        slot: Option<u16>,
        fps: &[(u16, u32)],
        p: u16,
        term: u32,
    ) {
        let Some(io) = &self.io else { return };
        if self.sealed {
            return;
        }
        let Some(slot) = slot else { return };
        if !self.src.migrates(slot) {
            return;
        }
        let _ = g;
        if self.src.is_walk_done()
            || fps
                .iter()
                .any(|&(s, fp)| s == slot && self.src.passed(s, fp))
        {
            let mut out = Vec::new();
            kv_repl::frame::encode(&Frame::Groups(enc.to_vec()), p, term, &mut out);
            io.send(out);
        }
    }
}

/// The destination side of one migration on one partition.
/// Stream bytes of one paced burst of a migration destination (with at most `WALK_KEYS`
/// actions): a fixed step, like the walks' listing steps.
const BURST_BYTES: usize = 256 << 10;

pub struct MigIn {
    pub id: u64,
    pub phase: MigPhase,
    /// The migrating slots of this partition, in slot order.
    slots: Vec<u16>,
    /// Keys the current copy wrote or deleted (the merge prune keeps exactly these).
    confirmed: KeySet,
    /// After `SYNC_DONE`: the prune walk and the `copy_end` it ends with; later actions wait.
    prune: Option<(KeyWalk, Seq)>,
    held: VecDeque<MemberAction>,
    /// Pacing of the copy's own writes by position barriers (LC-36): the actions that arrived
    /// while the writes submitted before them are not committed on this shard's ISR yet, in
    /// stream order, with their stream bytes (handed back to the transport when they are
    /// released, so that its admission stops reading the source meanwhile). The copy can
    /// never run ahead of this shard's members by more than one burst, so its own writes
    /// never make them lag (§23.8 step 4).
    paced: VecDeque<(MemberAction, usize)>,
    /// The last item of the burst in flight; once every item up to it executed, the highest
    /// seq they wrote is the barrier the commit point reaches before the next burst.
    burst_last: Option<u64>,
    barrier: Seq,
    src_addr: SocketAddr,
    pub io: Option<StreamIo>,
    pub dial_tag: Option<u64>,
    next_dial_ns: u64,
    epoch: u64,
    dest: MigrationDest,
    markers: Markers,
    stream: MemberStream,
    acked: Seq,
    sealed_reported: bool,
    last_rx_ns: u64,
}

impl MigIn {
    fn new(id: u64, phase: MigPhase, src_addr: SocketAddr, p: u16, slots: Vec<u16>) -> Self {
        let ring = Backlog::try_new(64 << 10).unwrap_or_else(|_| Backlog::new(4096));
        MigIn {
            id,
            phase,
            slots,
            confirmed: KeySet::default(),
            prune: None,
            held: VecDeque::new(),
            paced: VecDeque::new(),
            burst_last: None,
            barrier: Seq::ZERO,
            src_addr,
            io: None,
            dial_tag: None,
            next_dial_ns: 0,
            epoch: 0,
            dest: MigrationDest::new(id),
            markers: Markers::new(),
            stream: MemberStream::with_backlog(p, ring),
            acked: Seq::ZERO,
            sealed_reported: false,
            last_rx_ns: 0,
        }
    }

    pub fn abort(self) {
        if let Some(io) = &self.io {
            io.abort();
        }
    }

    pub fn connected(&mut self, io: StreamIo, now: u64) {
        self.last_rx_ns = now;
        self.dial_tag = None;
        self.io = Some(io);
        self.epoch += 1;
        self.dest = MigrationDest::new(self.id);
        self.markers = Markers::new();
        self.stream.restart(Seq::ZERO, Seq::ZERO);
        self.acked = Seq::ZERO;
        self.sealed_reported = false;
        // a new copy confirms its keys again (a merge over what an earlier copy left)
        self.confirmed = KeySet::default();
        self.prune = None;
        self.held.clear();
        self.paced.clear();
        self.burst_last = None;
        self.barrier = Seq::ZERO;
    }

    /// Whether the actions arriving now must wait (a burst is in flight, or its writes are
    /// not committed yet).
    fn gate_closed(&self, commit: Option<Seq>) -> bool {
        self.burst_last.is_some() || commit.is_none_or(|c| c < self.barrier) || !self.paced.is_empty()
    }

    pub fn dial_failed(&mut self, now: u64) {
        self.dial_tag = None;
        self.next_dial_ns = now + REDIAL_NS;
    }

    pub fn closed(&mut self, stream: u64, now: u64) {
        if self.io.as_ref().is_some_and(|i| i.id == stream) {
            self.io = None;
            self.epoch += 1;
            self.next_dial_ns = now + REDIAL_NS;
        }
    }
}

fn frame_bytes(f: &Frame, p: u16, term: u32) -> Vec<u8> {
    let mut out = Vec::new();
    kv_repl::frame::encode(f, p, term, &mut out);
    out
}

/// The key an op writes or deletes, if any.
fn op_key(o: &Op) -> Option<(u8, &[u8])> {
    match o {
        Op::Put { db, key, .. }
        | Op::Del { db, key }
        | Op::Expired { db, key }
        | Op::Expire { db, key, .. }
        | Op::Append { db, key, .. }
        | Op::SetRange { db, key, .. } => Some((*db, key)),
        _ => None,
    }
}

fn ops_keys(ops: &[Op]) -> SmallVec<[(Box<[u8]>, Access); 2]> {
    let mut keys: SmallVec<[(Box<[u8]>, Access); 2]> = SmallVec::new();
    for op in ops {
        let (k, a) = match op {
            Op::Put { key, .. } | Op::Del { key, .. } | Op::Expired { key, .. } => {
                (key, Access::Write)
            }
            Op::Expire { key, .. } | Op::Append { key, .. } | Op::SetRange { key, .. } => {
                (key, Access::ReadWrite)
            }
            _ => continue,
        };
        if !keys.iter().any(|(x, _)| x == k) {
            keys.push((k.clone(), a));
        }
    }
    keys
}

impl<B: Backend> Worker<B> {
    /// Reconciles partition `p`'s migration endpoints with the assignment's tasks.
    pub(super) fn sync_migrations(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        tasks: &[MigrationTask],
    ) {
        let parts = self.shared.partitions.max(1);
        let rate = cw.sync_iops;
        let Some(pr) = cw.part(p) else { return };
        let mine = |t: &MigrationTask| -> Vec<u16> {
            t.slots().iter_slots().filter(|s| s % parts == p).collect()
        };
        let active =
            |ph: MigPhase| matches!(ph, MigPhase::Copy | MigPhase::Ready | MigPhase::Cutover);
        let mut keep_out = Vec::new();
        let mut keep_in = Vec::new();
        for t in tasks {
            let slots = mine(t);
            if slots.is_empty() {
                continue;
            }
            match t {
                MigrationTask::Out {
                    id,
                    phase,
                    barriers,
                    ..
                } if active(*phase) => {
                    keep_out.push(*id);
                    match pr.mig_out.iter_mut().find(|m| m.id == *id) {
                        Some(m) => m.phase = *phase,
                        None => pr
                            .mig_out
                            .push(MigOut::new(*id, *phase, slots, rate, *barriers)),
                    }
                }
                MigrationTask::In {
                    id,
                    phase,
                    src_repl,
                    ..
                } if active(*phase) => {
                    keep_in.push(*id);
                    match pr
                        .mig_in
                        .iter_mut()
                        .find(|m| m.id == *id && m.src_addr == src_repl.0)
                    {
                        Some(m) => m.phase = *phase,
                        None => {
                            if let Some(i) = pr.mig_in.iter().position(|m| m.id == *id) {
                                pr.mig_in.remove(i).abort();
                            }
                            pr.mig_in
                                .push(MigIn::new(*id, *phase, src_repl.0, p, slots));
                        }
                    }
                }
                _ => {}
            }
        }
        let (gone_out, kept_out): (Vec<MigOut>, Vec<MigOut>) = pr
            .mig_out
            .drain(..)
            .partition(|m| !keep_out.contains(&m.id));
        pr.mig_out = kept_out;
        for m in gone_out {
            m.abort();
        }
        let (gone_in, kept_in): (Vec<MigIn>, Vec<MigIn>) =
            pr.mig_in.drain(..).partition(|m| !keep_in.contains(&m.id));
        pr.mig_in = kept_in;
        for m in gone_in {
            m.abort();
        }
        let live: Vec<u64> = pr
            .mig_out
            .iter()
            .filter_map(|m| m.io.as_ref().map(|i| i.id))
            .chain(pr.mig_in.iter().filter_map(|m| m.io.as_ref().map(|i| i.id)))
            .collect();
        cw.streams.retain(|sid, r| match r {
            StreamRef::MigOut { p: x, .. } | StreamRef::MigIn { p: x, .. } => {
                *x != p || live.contains(sid)
            }
            _ => true,
        });
    }

    // -----------------------------------------------------------------------------------------
    // source

    pub(super) fn mig_out_hello(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        id: u64,
        io: StreamIo,
        h: Hello,
    ) {
        let ident = cw.cs.identity();
        let last = before(self.backend.next_seq(p));
        let reject = |io: StreamIo, r: RejectReason| io.send(frame_bytes(&Frame::Reject(r), p, 0));
        if !same_token(&h.auth, &cw.cs.auth) || ident.cluster_id != Some(h.cluster_id) {
            return reject(io, RejectReason::BadAuth);
        }
        let term = cw
            .assignment()
            .and_then(|a| a.role.as_ref())
            .filter(|r| r.is_primary)
            .map(|r| r.term);
        let Some(term) = term else {
            return reject(io, RejectReason::NotPrimary);
        };
        let Some(pr) = cw.part(p) else { return };
        let Some(mo) = pr.mig_out.iter_mut().find(|m| m.id == id) else {
            return reject(io, RejectReason::NotMember);
        };
        let sid = io.id;
        io.send(frame_bytes(
            &Frame::Accept {
                term,
                mode: AcceptMode::Full { at: last },
            },
            p,
            term,
        ));
        mo.restart(io);
        cw.streams.insert(sid, StreamRef::MigOut { p, id });
        tracing::info!(
            partition = p,
            migration = id,
            "migration destination connected: copy starts"
        );
    }

    pub(super) fn mig_out_frame(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        id: u64,
        stream: u64,
        f: Frame,
    ) {
        let last = before(self.backend.next_seq(p));
        let Some(pr) = cw.part(p) else { return };
        let Some(mo) = pr.mig_out.iter_mut().find(|m| m.id == id) else {
            return;
        };
        if mo.io.as_ref().is_none_or(|i| i.id != stream) {
            return;
        }
        if let Frame::Ack { durable, .. } = f {
            // position barriers only: copy_end and each further barrier fixed when recorded
            if mo.src.on_dest_committed(durable, last) && !mo.ready_reported {
                mo.ready_reported = true;
                tracing::info!(partition = p, migration = id, "migration barriers complete");
                cw.cs.tell_agent(AgentMsg::MigReady { id, partition: p });
            }
        }
    }

    pub(super) fn drive_mig_out(&mut self, cw: &mut ClusterWorker, p: u16) {
        let now_ns = self.now.mono_ns;
        let now = self.now.boot_ns;
        let last = before(self.backend.next_seq(p));
        let term = cw
            .assignment()
            .and_then(|a| a.role.as_ref())
            .map_or(0, |r| r.term);
        let lens = |w: &Self, s: u16| w.backend.slot_len(p, 0, s);
        let cs = cw.cs.clone();
        let mut submits: Vec<(Request, ReplJob)> = Vec::new();
        {
            let Some(pr) = cw.part(p) else { return };
            for mo in pr.mig_out.iter_mut() {
                let Some(io) = &mo.io else { continue };
                if io.unsent() >= SEND_WINDOW || mo.reading || mo.walk.busy {
                    continue;
                }
                if !mo.walk.done() && !mo.begun {
                    mo.walk.skip_empty(&|s| lens(self, s));
                    mo.src.advance(mo.walk.pos());
                }
                if mo.walk.done() {
                    if !mo.copy_end_sent {
                        mo.src.walk_done(last);
                        io.send(frame_bytes(&Frame::SyncDone { at: last }, p, term));
                        mo.copy_end_sent = true;
                        mo.pos_sent = last;
                    } else if !mo.sealed
                        && (last > mo.pos_sent
                            || now.saturating_sub(mo.pos_sent_ns) >= POSITION_REPEAT_NS)
                    {
                        // a position marker when the source moved, and periodically (liveness)
                        io.send(frame_bytes(&Frame::Commit(last.max(mo.pos_sent)), p, term));
                        mo.pos_sent = last.max(mo.pos_sent);
                        mo.pos_sent_ns = now;
                    }
                    if mo.phase == MigPhase::Cutover && !mo.sealed {
                        // after every earlier frame of these slots (a single position barrier)
                        io.send(frame_bytes(&Frame::Seal { mig: mo.id, last }, p, term));
                        mo.sealed = true;
                        tracing::info!(partition = p, migration = mo.id, "migration sealed");
                    }
                    // a destination already past every barrier at the walk end
                    if !mo.ready_reported && mo.src.is_ready() {
                        mo.ready_reported = true;
                        cs.tell_agent(AgentMsg::MigReady {
                            id: mo.id,
                            partition: p,
                        });
                    }
                    continue;
                }
                if !mo.bucket.try_take(now_ns, u64::from(WALK_KEYS)) {
                    continue;
                }
                let slot = mo.walk.slots[mo.walk.idx];
                if !mo.begun {
                    io.send(frame_bytes(&Frame::SnapBegin { slot }, p, term));
                    mo.begun = true;
                    mo.count = 0;
                }
                mo.walk.busy = true;
                let req = Request {
                    db: 0,
                    keys: SmallVec::new(),
                    kind: ReqKind::SlotIter {
                        slot,
                        fp_from: mo.walk.fp_from as u32,
                        max_entries: WALK_KEYS,
                    },
                    origin: Origin::Client,
                };
                submits.push((
                    req,
                    ReplJob {
                        work: ReplWork::Walk,
                        then: Then::Listed {
                            walker: Walker::MigOut {
                                id: mo.id,
                                epoch: mo.epoch,
                            },
                        },
                    },
                ));
            }
        }
        for (req, rj) in submits {
            self.submit_repl(cw, p, req, rj);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn mig_out_listed(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        id: u64,
        epoch: u64,
        next: u64,
        keys: Vec<Box<[u8]>>,
        failed: bool,
    ) {
        let term = cw
            .assignment()
            .and_then(|a| a.role.as_ref())
            .map_or(0, |r| r.term);
        let mut submit = None;
        {
            let Some(pr) = cw.part(p) else { return };
            let Some(mo) = pr.mig_out.iter_mut().find(|m| m.id == id) else {
                return;
            };
            if mo.epoch != epoch {
                return;
            }
            mo.walk.busy = false;
            if failed {
                if let Some(io) = &mo.io {
                    io.abort();
                }
                return;
            }
            let slot = mo.walk.slots[mo.walk.idx];
            mo.walk.listed(next);
            mo.src.advance(mo.walk.pos());
            if keys.is_empty() {
                if mo.walk.fp_from == 0
                    && let Some(io) = &mo.io
                {
                    io.send(frame_bytes(
                        &Frame::SnapEnd {
                            slot,
                            count: mo.count,
                        },
                        p,
                        term,
                    ));
                    mo.begun = false;
                }
            } else {
                mo.reading = true;
                let req = Request {
                    db: 0,
                    keys: keys.iter().map(|k| (k.clone(), Access::Read)).collect(),
                    kind: ReqKind::Keyed,
                    origin: Origin::Client,
                };
                submit = Some((
                    req,
                    ReplJob {
                        work: ReplWork::Read { db: 0, keys },
                        then: Then::MigOutRead { id, epoch },
                    },
                ));
            }
        }
        if let Some((req, rj)) = submit {
            self.submit_repl(cw, p, req, rj);
        }
    }

    pub(super) fn mig_out_read(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        id: u64,
        epoch: u64,
        work: &ReplWork,
        res: Result<ReplOut, StoreError>,
    ) {
        let term = cw
            .assignment()
            .and_then(|a| a.role.as_ref())
            .map_or(0, |r| r.term);
        let Some(pr) = cw.part(p) else { return };
        let Some(mo) = pr.mig_out.iter_mut().find(|m| m.id == id) else {
            return;
        };
        if mo.epoch != epoch {
            return;
        }
        mo.reading = false;
        let (Some(io), ReplWork::Read { db, keys }) = (&mo.io, work) else {
            return;
        };
        let Ok(ReplOut::Images(images)) = res else {
            io.abort();
            return;
        };
        let mut out = Vec::new();
        for (k, img) in keys.iter().zip(images) {
            if let Some((vtype, expire_at, value)) = img {
                kv_repl::frame::encode(
                    &Frame::SnapRec(SnapRec {
                        db: *db,
                        vtype,
                        expire_at,
                        key: k.clone(),
                        value,
                    }),
                    p,
                    term,
                    &mut out,
                );
                mo.count += 1;
            }
        }
        if mo.walk.fp_from == 0 && mo.begun {
            let slot = mo.walk.slots[mo.walk.idx.saturating_sub(1)];
            kv_repl::frame::encode(
                &Frame::SnapEnd {
                    slot,
                    count: mo.count,
                },
                p,
                term,
                &mut out,
            );
            mo.begun = false;
        }
        io.send(out);
    }

    // -----------------------------------------------------------------------------------------
    // destination

    pub(super) fn drive_mig_in(&mut self, cw: &mut ClusterWorker, p: u16) {
        let now = self.now.boot_ns;
        let ident = cw.cs.identity();
        let auth = cw.cs.auth.clone();
        let (shard, term) = cw
            .assignment()
            .and_then(|a| a.role.as_ref())
            .map_or((0, 0), |r| (r.shard.0, r.term));
        let cs = cw.cs.clone();
        let net = cs.net.get();
        let mut tag = cw.next_tag;
        let commit = cw.part(p).and_then(|pr| pr.primary_commit());
        let mut reports = Vec::new();
        let mut prune_steps: Vec<(Request, ReplJob)> = Vec::new();
        let mut released: Vec<(u64, Vec<MemberAction>)> = Vec::new();
        let mut paced_bursts: Vec<(u64, Vec<MemberAction>)> = Vec::new();
        let lens = |w: &Self, s: u16| w.backend.slot_len(p, 0, s);
        {
            let Some(pr) = cw.part(p) else { return };
            for mi in pr.mig_in.iter_mut() {
                // paced actions go on once the burst before them is committed, one fixed step
                // at a time (at most WALK_KEYS actions or BURST_BYTES of stream bytes)
                if mi.io.is_some()
                    && mi.burst_last.is_none()
                    && !mi.paced.is_empty()
                    && commit.is_some_and(|c| c >= mi.barrier)
                {
                    let mut bytes = 0usize;
                    let mut acts: Vec<MemberAction> = Vec::new();
                    while let Some((a, b)) = mi.paced.pop_front() {
                        bytes += b;
                        acts.push(a);
                        if acts.len() >= WALK_KEYS as usize || bytes >= BURST_BYTES {
                            break;
                        }
                    }
                    if let Some(io) = &mi.io {
                        io.consumed(bytes);
                    }
                    paced_bursts.push((mi.id, acts));
                }
                // the merge prune after the copy: one listing step at a time
                if mi.io.is_some()
                    && let Some((walk, end)) = &mut mi.prune
                    && !walk.busy
                {
                    walk.skip_empty(&|s| lens(self, s));
                    if walk.done() {
                        mi.markers.marker(*end);
                        mi.prune = None;
                        released.push((mi.id, mi.held.drain(..).collect()));
                    } else {
                        walk.busy = true;
                        let slot = walk.slots[walk.idx];
                        prune_steps.push((
                            Request {
                                db: 0,
                                keys: SmallVec::new(),
                                kind: ReqKind::SlotIter {
                                    slot,
                                    fp_from: walk.fp_from as u32,
                                    max_entries: WALK_KEYS,
                                },
                                origin: Origin::Client,
                            },
                            ReplJob {
                                work: ReplWork::Walk,
                                then: Then::Listed {
                                    walker: Walker::MigInPrune {
                                        id: mi.id,
                                        epoch: mi.epoch,
                                    },
                                },
                            },
                        ));
                    }
                }
                match &mi.io {
                    None => {
                        if mi.dial_tag.is_some() || now < mi.next_dial_ns {
                            continue;
                        }
                        let (Some(cluster_id), Some(node), Some(net)) =
                            (ident.cluster_id, ident.node_id, net)
                        else {
                            continue;
                        };
                        let h = Hello {
                            cluster_id,
                            auth: auth.clone(),
                            node_id: node.0,
                            shard,
                            partition: p,
                            fence_term: ident.fence_term,
                            last_term: term,
                            applied: Seq::ZERO,
                            recovered: Recovered::Clean,
                            learner: false,
                            migration: Some(mi.id),
                        };
                        mi.dial_tag = Some(tag);
                        mi.next_dial_ns = now + REDIAL_NS;
                        net.dial(
                            self.id,
                            p,
                            tag,
                            mi.src_addr,
                            frame_bytes(&Frame::Hello(h), p, term),
                        );
                        tag += 1;
                    }
                    Some(io) => {
                        if now.saturating_sub(mi.last_rx_ns) >= DEST_IDLE_NS {
                            tracing::info!(
                                partition = p,
                                migration = mi.id,
                                "no frame from the migration source: redialing"
                            );
                            io.abort();
                            continue;
                        }
                        // acknowledge the source positions whose effects are committed here
                        let Some(commit) = commit else { continue };
                        if let Some(pos) = mi.markers.acked(commit) {
                            mi.dest.on_src_seq_committed(pos);
                            if pos > mi.acked {
                                mi.acked = pos;
                                io.send(frame_bytes(
                                    &Frame::Ack {
                                        durable: pos,
                                        applied: pos,
                                    },
                                    p,
                                    term,
                                ));
                            }
                        }
                        if mi.dest.sealed() && !mi.sealed_reported {
                            mi.sealed_reported = true;
                            tracing::info!(
                                partition = p,
                                migration = mi.id,
                                "migration seal committed"
                            );
                            reports.push(AgentMsg::Sealed {
                                id: mi.id,
                                partition: p,
                            });
                        }
                    }
                }
            }
        }
        cw.next_tag = tag;
        for r in reports {
            cs.tell_agent(r);
        }
        for (req, rj) in prune_steps {
            self.submit_repl(cw, p, req, rj);
        }
        for (id, acts) in released {
            self.mig_in_actions(cw, p, id, acts);
        }
        for (id, acts) in paced_bursts {
            self.mig_in_burst(cw, p, id, acts);
        }
    }

    /// A listing step of the merge prune: deletes the keys the copy did not confirm (own
    /// writes of the destination, tracked like every copy item).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn mig_in_prune_listed(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        id: u64,
        epoch: u64,
        next: u64,
        keys: Vec<Box<[u8]>>,
        failed: bool,
    ) {
        let mut submit = None;
        {
            let Some(pr) = cw.part(p) else { return };
            let Some(mi) = pr.mig_in.iter_mut().find(|m| m.id == id) else {
                return;
            };
            if mi.epoch != epoch {
                return;
            }
            let Some((walk, _)) = &mut mi.prune else {
                return;
            };
            walk.busy = false;
            if failed {
                // the copy starts again on a new stream
                if let Some(io) = mi.io.take() {
                    io.abort();
                }
                return;
            }
            walk.listed(next);
            let doomed: Vec<Box<[u8]>> = keys
                .into_iter()
                .filter(|k| !mi.confirmed.contains(0, k))
                .collect();
            if !doomed.is_empty() {
                let item = mi.markers.item();
                let req = Request {
                    db: 0,
                    keys: doomed.iter().map(|k| (k.clone(), Access::Write)).collect(),
                    kind: ReqKind::Keyed,
                    origin: Origin::Client,
                };
                let ops = doomed
                    .into_iter()
                    .map(|key| Op::Del { db: 0, key })
                    .collect();
                submit = Some((
                    req,
                    ReplJob {
                        work: ReplWork::Own { ops },
                        then: Then::MigInApply { id, epoch, item },
                    },
                ));
            }
        }
        if let Some((req, rj)) = submit {
            self.submit_repl(cw, p, req, rj);
        }
    }

    /// Returns whether the frame's stream bytes are handled now (false: they wait with its
    /// paced actions and are handed back when those are released).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn mig_in_frame(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        id: u64,
        stream: u64,
        _term: u32,
        f: Frame,
        bytes: usize,
    ) -> bool {
        let now = self.now.boot_ns;
        let acts = {
            let Some(pr) = cw.part(p) else { return true };
            let commit = pr.primary_commit();
            let Some(mi) = pr.mig_in.iter_mut().find(|m| m.id == id) else {
                return true;
            };
            if mi.io.as_ref().is_none_or(|i| i.id != stream) {
                return true;
            }
            mi.last_rx_ns = now;
            if let Frame::Reject(r) = &f {
                tracing::info!(
                    partition = p,
                    migration = id,
                    "the migration source refused the stream: {r:?}"
                );
                if let Some(io) = mi.io.take() {
                    io.abort();
                }
                return true;
            }
            let mut acts = Vec::new();
            match mi.stream.on_frame(f) {
                Ok(a) => acts.push(a),
                Err(_) => {
                    if let Some(io) = mi.io.take() {
                        io.abort();
                    }
                    return true;
                }
            }
            while let Some(a) = mi.stream.next_action() {
                acts.push(a);
            }
            if mi.gate_closed(commit) {
                // the bytes of the frame go back with its last action
                let n = acts.len();
                for (i, a) in acts.into_iter().enumerate() {
                    mi.paced.push_back((a, if i + 1 == n { bytes } else { 0 }));
                }
                return false;
            }
            acts
        };
        self.mig_in_burst(cw, p, id, acts);
        true
    }

    /// Submits `acts` as one burst: the next actions wait until its writes are committed.
    fn mig_in_burst(&mut self, cw: &mut ClusterWorker, p: u16, id: u64, acts: Vec<MemberAction>) {
        let before = cw
            .part(p)
            .and_then(|pr| pr.mig_in.iter().find(|m| m.id == id))
            .map(|mi| mi.markers.next_item());
        self.mig_in_actions(cw, p, id, acts);
        if let Some(pr) = cw.part(p)
            && let Some(mi) = pr.mig_in.iter_mut().find(|m| m.id == id)
            && let Some(before) = before
        {
            let after = mi.markers.next_item();
            if after > before && mi.markers.executed_below() < after {
                mi.burst_last = Some(after - 1);
            }
        }
    }

    /// Applies destination actions in stream order. From `SYNC_DONE` on they wait until the
    /// merge prune deleted every key of the migrating slots the copy did not confirm.
    fn mig_in_actions(&mut self, cw: &mut ClusterWorker, p: u16, id: u64, acts: Vec<MemberAction>) {
        let mut submits: Vec<(Request, ReplJob)> = Vec::new();
        {
            let Some(pr) = cw.part(p) else { return };
            let Some(mi) = pr.mig_in.iter_mut().find(|m| m.id == id) else {
                return;
            };
            let epoch = mi.epoch;
            for a in acts {
                if mi.prune.is_some() {
                    mi.held.push_back(a);
                    continue;
                }
                let ops = match a {
                    MemberAction::Snap(rec, _) => {
                        mi.confirmed.insert(rec.db, &rec.key);
                        vec![Op::Put {
                            db: rec.db,
                            vtype: rec.vtype,
                            expire_at: rec.expire_at,
                            key: rec.key,
                            value: rec.value,
                        }]
                    }
                    MemberAction::Apply(g) => {
                        let ops: Vec<Op> = g
                            .ops
                            .into_iter()
                            .filter(|o| {
                                !matches!(o, Op::FlushDb { .. } | Op::FlushSlot { .. } | Op::Noop)
                            })
                            .collect();
                        for o in &ops {
                            if let Some((db, k)) = op_key(o) {
                                mi.confirmed.insert(db, k);
                            }
                        }
                        ops
                    }
                    MemberAction::SyncDone(at) => {
                        // the copy is complete: prune what it did not confirm, then `copy_end`
                        mi.prune = Some((KeyWalk::new(mi.slots.clone()), at));
                        continue;
                    }
                    MemberAction::Commit(at) => {
                        mi.markers.marker(at);
                        continue;
                    }
                    MemberAction::Seal { last, .. } => {
                        mi.markers.marker(last);
                        mi.dest.on_seal(last);
                        continue;
                    }
                    _ => continue,
                };
                if ops.is_empty() {
                    continue;
                }
                let item = mi.markers.item();
                let req = Request {
                    db: 0,
                    keys: ops_keys(&ops),
                    kind: ReqKind::Keyed,
                    origin: Origin::Client,
                };
                submits.push((
                    req,
                    ReplJob {
                        work: ReplWork::Own { ops },
                        then: Then::MigInApply { id, epoch, item },
                    },
                ));
            }
        }
        for (req, rj) in submits {
            self.submit_repl(cw, p, req, rj);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn mig_in_applied(
        &mut self,
        cw: &mut ClusterWorker,
        p: u16,
        id: u64,
        epoch: u64,
        item: u64,
        wrote: Option<Seq>,
        ok: bool,
    ) {
        let Some(pr) = cw.part(p) else { return };
        let Some(mi) = pr.mig_in.iter_mut().find(|m| m.id == id) else {
            return;
        };
        if mi.epoch != epoch {
            return;
        }
        if !ok {
            // the copy starts again from the beginning on a new stream
            if let Some(io) = mi.io.take() {
                io.abort();
            }
            return;
        }
        mi.markers.executed(item, wrote);
        if mi.burst_last.is_some_and(|l| mi.markers.executed_below() > l) {
            // the burst executed: the next one waits until its writes are committed
            mi.burst_last = None;
            mi.barrier = mi.barrier.max(mi.markers.written());
        }
    }
}
