//! Migration endpoints of the node's partitions (DESIGN §23.13).
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
//! those positions only (LC-36). The number of further barriers is not part of the pushed
//! assignment; the source uses the default of `kvadm migrate --barriers` (2).

use std::net::SocketAddr;

use kv_common::Access;
use kv_common::pace::TokenBucket;
use kv_ctrl::{MigPhase, MigrationTask};
use kv_repl::frame::{AcceptMode, Frame, Hello, Recovered, RejectReason, SnapRec};
use kv_repl::member::{MemberAction, MemberStream};
use kv_repl::migrate::{MigrationDest, MigrationSource};
use kv_repl::backlog::Backlog;
use kv_storage::effect::{Group, Op};
use kv_storage::keyspace::StoreError;
use kv_storage::layout::Seq;
use kv_storage::partition::{Origin, ReqKind, Request};
use smallvec::SmallVec;

use super::net::StreamIo;
use super::repl_glue::{ClusterWorker, KeyWalk, ReplJob, SEND_WINDOW, StreamRef, Then, WALK_KEYS, Walker};
use super::track::Markers;
use super::{AgentMsg, before, same_token};
use crate::runtime::backend::{Backend, ReplOut, ReplWork};
use crate::runtime::worker::Worker;

/// Further barriers after `copy_end` (`kvadm migrate --barriers`, default).
const BARRIERS: u8 = 2;
/// Pause before a destination redials its source.
const REDIAL_NS: u64 = 200_000_000;

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
}

impl MigOut {
    fn new(id: u64, phase: MigPhase, slots: Vec<u16>, rate: u64) -> Self {
        let rate = rate.max(1);
        MigOut {
            id,
            phase,
            src: MigrationSource::new(id, &slots, BARRIERS),
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
        }
    }

    /// A new destination stream: the copy starts from the beginning.
    fn restart(&mut self, io: StreamIo) {
        if let Some(old) = self.io.replace(io) {
            old.abort();
        }
        self.epoch += 1;
        self.src = MigrationSource::new(self.id, &self.slots, BARRIERS);
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
    pub fn on_group(&mut self, g: &Group, enc: &[u8], slot: Option<u16>, fps: &[(u16, u32)], p: u16, term: u32) {
        let Some(io) = &self.io else { return };
        if self.sealed {
            return;
        }
        let Some(slot) = slot else { return };
        if !self.src.migrates(slot) {
            return;
        }
        let _ = g;
        if self.src.is_walk_done() || fps.iter().any(|&(s, fp)| s == slot && self.src.passed(s, fp)) {
            let mut out = Vec::new();
            kv_repl::frame::encode(&Frame::Groups(enc.to_vec()), p, term, &mut out);
            io.send(out);
        }
    }
}

/// The destination side of one migration on one partition.
pub struct MigIn {
    pub id: u64,
    pub phase: MigPhase,
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
}

impl MigIn {
    fn new(id: u64, phase: MigPhase, src_addr: SocketAddr, p: u16) -> Self {
        let ring = Backlog::try_new(64 << 10).unwrap_or_else(|_| Backlog::new(4096));
        MigIn {
            id,
            phase,
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
        }
    }

    pub fn abort(self) {
        if let Some(io) = &self.io {
            io.abort();
        }
    }

    pub fn connected(&mut self, io: StreamIo) {
        self.dial_tag = None;
        self.io = Some(io);
        self.epoch += 1;
        self.dest = MigrationDest::new(self.id);
        self.markers = Markers::new();
        self.stream.restart(Seq::ZERO, Seq::ZERO);
        self.acked = Seq::ZERO;
        self.sealed_reported = false;
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

fn ops_keys(ops: &[Op]) -> SmallVec<[(Box<[u8]>, Access); 2]> {
    let mut keys: SmallVec<[(Box<[u8]>, Access); 2]> = SmallVec::new();
    for op in ops {
        let (k, a) = match op {
            Op::Put { key, .. } | Op::Del { key, .. } | Op::Expired { key, .. } => (key, Access::Write),
            Op::Expire { key, .. } | Op::Append { key, .. } | Op::SetRange { key, .. } => (key, Access::ReadWrite),
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
    pub(super) fn sync_migrations(&mut self, cw: &mut ClusterWorker, p: u16, tasks: &[MigrationTask]) {
        let parts = self.shared.partitions.max(1);
        let rate = cw.sync_iops;
        let Some(pr) = cw.part(p) else { return };
        let mine = |t: &MigrationTask| -> Vec<u16> { t.slots().iter_slots().filter(|s| s % parts == p).collect() };
        let active = |ph: MigPhase| matches!(ph, MigPhase::Copy | MigPhase::Ready | MigPhase::Cutover);
        let mut keep_out = Vec::new();
        let mut keep_in = Vec::new();
        for t in tasks {
            let slots = mine(t);
            if slots.is_empty() {
                continue;
            }
            match t {
                MigrationTask::Out { id, phase, .. } if active(*phase) => {
                    keep_out.push(*id);
                    match pr.mig_out.iter_mut().find(|m| m.id == *id) {
                        Some(m) => m.phase = *phase,
                        None => pr.mig_out.push(MigOut::new(*id, *phase, slots, rate)),
                    }
                }
                MigrationTask::In { id, phase, src_repl, .. } if active(*phase) => {
                    keep_in.push(*id);
                    match pr.mig_in.iter_mut().find(|m| m.id == *id && m.src_addr == src_repl.0) {
                        Some(m) => m.phase = *phase,
                        None => {
                            if let Some(i) = pr.mig_in.iter().position(|m| m.id == *id) {
                                pr.mig_in.remove(i).abort();
                            }
                            pr.mig_in.push(MigIn::new(*id, *phase, src_repl.0, p));
                        }
                    }
                }
                _ => {}
            }
        }
        let (gone_out, kept_out): (Vec<MigOut>, Vec<MigOut>) = pr.mig_out.drain(..).partition(|m| !keep_out.contains(&m.id));
        pr.mig_out = kept_out;
        for m in gone_out {
            m.abort();
        }
        let (gone_in, kept_in): (Vec<MigIn>, Vec<MigIn>) = pr.mig_in.drain(..).partition(|m| !keep_in.contains(&m.id));
        pr.mig_in = kept_in;
        for m in gone_in {
            m.abort();
        }
        let live: Vec<u64> = pr.mig_out.iter().filter_map(|m| m.io.as_ref().map(|i| i.id)).chain(pr.mig_in.iter().filter_map(|m| m.io.as_ref().map(|i| i.id))).collect();
        cw.streams.retain(|sid, r| match r {
            StreamRef::MigOut { p: x, .. } | StreamRef::MigIn { p: x, .. } => *x != p || live.contains(sid),
            _ => true,
        });
    }

    // -----------------------------------------------------------------------------------------
    // source

    pub(super) fn mig_out_hello(&mut self, cw: &mut ClusterWorker, p: u16, id: u64, io: StreamIo, h: Hello) {
        let ident = cw.cs.identity();
        let last = before(self.backend.next_seq(p));
        let reject = |io: StreamIo, r: RejectReason| io.send(frame_bytes(&Frame::Reject(r), p, 0));
        if !same_token(&h.auth, &cw.cs.auth) || ident.cluster_id != Some(h.cluster_id) {
            return reject(io, RejectReason::BadAuth);
        }
        let term = cw.assignment().and_then(|a| a.role.as_ref()).filter(|r| r.is_primary).map(|r| r.term);
        let Some(term) = term else { return reject(io, RejectReason::NotPrimary) };
        let Some(pr) = cw.part(p) else { return };
        let Some(mo) = pr.mig_out.iter_mut().find(|m| m.id == id) else { return reject(io, RejectReason::NotMember) };
        let sid = io.id;
        io.send(frame_bytes(&Frame::Accept { term, mode: AcceptMode::Full { at: last } }, p, term));
        mo.restart(io);
        cw.streams.insert(sid, StreamRef::MigOut { p, id });
        tracing::info!(partition = p, migration = id, "migration destination connected: copy starts");
    }

    pub(super) fn mig_out_frame(&mut self, cw: &mut ClusterWorker, p: u16, id: u64, stream: u64, f: Frame) {
        let last = before(self.backend.next_seq(p));
        let Some(pr) = cw.part(p) else { return };
        let Some(mo) = pr.mig_out.iter_mut().find(|m| m.id == id) else { return };
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
        let last = before(self.backend.next_seq(p));
        let term = cw.assignment().and_then(|a| a.role.as_ref()).map_or(0, |r| r.term);
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
                if !mo.walk.done() {
                    if !mo.begun {
                        mo.walk.skip_empty(&|s| lens(self, s));
                        mo.src.advance(mo.walk.pos());
                    }
                }
                if mo.walk.done() {
                    if !mo.copy_end_sent {
                        mo.src.walk_done(last);
                        io.send(frame_bytes(&Frame::SyncDone { at: last }, p, term));
                        mo.copy_end_sent = true;
                        mo.pos_sent = last;
                    } else if last > mo.pos_sent && !mo.sealed {
                        io.send(frame_bytes(&Frame::Commit(last), p, term));
                        mo.pos_sent = last;
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
                        cs.tell_agent(AgentMsg::MigReady { id: mo.id, partition: p });
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
                let req = Request { db: 0, keys: SmallVec::new(), kind: ReqKind::SlotIter { slot, fp_from: mo.walk.fp_from as u32, max_entries: WALK_KEYS }, origin: Origin::Client };
                submits.push((req, ReplJob { work: ReplWork::Walk, then: Then::Listed { walker: Walker::MigOut { id: mo.id, epoch: mo.epoch } } }));
            }
        }
        for (req, rj) in submits {
            self.submit_repl(cw, p, req, rj);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn mig_out_listed(&mut self, cw: &mut ClusterWorker, p: u16, id: u64, epoch: u64, next: u64, keys: Vec<Box<[u8]>>, failed: bool) {
        let term = cw.assignment().and_then(|a| a.role.as_ref()).map_or(0, |r| r.term);
        let mut submit = None;
        {
            let Some(pr) = cw.part(p) else { return };
            let Some(mo) = pr.mig_out.iter_mut().find(|m| m.id == id) else { return };
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
                    io.send(frame_bytes(&Frame::SnapEnd { slot, count: mo.count }, p, term));
                    mo.begun = false;
                }
            } else {
                mo.reading = true;
                let req = Request { db: 0, keys: keys.iter().map(|k| (k.clone(), Access::Read)).collect(), kind: ReqKind::Keyed, origin: Origin::Client };
                submit = Some((req, ReplJob { work: ReplWork::Read { db: 0, keys }, then: Then::MigOutRead { id, epoch } }));
            }
        }
        if let Some((req, rj)) = submit {
            self.submit_repl(cw, p, req, rj);
        }
    }

    pub(super) fn mig_out_read(&mut self, cw: &mut ClusterWorker, p: u16, id: u64, epoch: u64, work: &ReplWork, res: Result<ReplOut, StoreError>) {
        let term = cw.assignment().and_then(|a| a.role.as_ref()).map_or(0, |r| r.term);
        let Some(pr) = cw.part(p) else { return };
        let Some(mo) = pr.mig_out.iter_mut().find(|m| m.id == id) else { return };
        if mo.epoch != epoch {
            return;
        }
        mo.reading = false;
        let (Some(io), ReplWork::Read { db, keys }) = (&mo.io, work) else { return };
        let Ok(ReplOut::Images(images)) = res else {
            io.abort();
            return;
        };
        let mut out = Vec::new();
        for (k, img) in keys.iter().zip(images) {
            if let Some((vtype, expire_at, value)) = img {
                kv_repl::frame::encode(&Frame::SnapRec(SnapRec { db: *db, vtype, expire_at, key: k.clone(), value }), p, term, &mut out);
                mo.count += 1;
            }
        }
        if mo.walk.fp_from == 0 && mo.begun {
            let slot = mo.walk.slots[mo.walk.idx.saturating_sub(1)];
            kv_repl::frame::encode(&Frame::SnapEnd { slot, count: mo.count }, p, term, &mut out);
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
        let (shard, term) = cw.assignment().and_then(|a| a.role.as_ref()).map_or((0, 0), |r| (r.shard.0, r.term));
        let cs = cw.cs.clone();
        let net = cs.net.get();
        let mut tag = cw.next_tag;
        let commit = cw.part(p).and_then(|pr| pr.primary_commit());
        let mut reports = Vec::new();
        {
            let Some(pr) = cw.part(p) else { return };
            for mi in pr.mig_in.iter_mut() {
                match &mi.io {
                    None => {
                        if mi.dial_tag.is_some() || now < mi.next_dial_ns {
                            continue;
                        }
                        let (Some(cluster_id), Some(node), Some(net)) = (ident.cluster_id, ident.node_id, net) else { continue };
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
                        net.dial(self.id, p, tag, mi.src_addr, frame_bytes(&Frame::Hello(h), p, term));
                        tag += 1;
                    }
                    Some(io) => {
                        // acknowledge the source positions whose effects are committed here
                        let Some(commit) = commit else { continue };
                        if let Some(pos) = mi.markers.acked(commit) {
                            mi.dest.on_src_seq_committed(pos);
                            if pos > mi.acked {
                                mi.acked = pos;
                                io.send(frame_bytes(&Frame::Ack { durable: pos, applied: pos }, p, term));
                            }
                        }
                        if mi.dest.sealed() && !mi.sealed_reported {
                            mi.sealed_reported = true;
                            tracing::info!(partition = p, migration = mi.id, "migration seal committed");
                            reports.push(AgentMsg::Sealed { id: mi.id, partition: p });
                        }
                    }
                }
            }
        }
        cw.next_tag = tag;
        for r in reports {
            cs.tell_agent(r);
        }
    }

    pub(super) fn mig_in_frame(&mut self, cw: &mut ClusterWorker, p: u16, id: u64, stream: u64, _term: u32, f: Frame) {
        let mut submits: Vec<(Request, ReplJob)> = Vec::new();
        {
            let Some(pr) = cw.part(p) else { return };
            let Some(mi) = pr.mig_in.iter_mut().find(|m| m.id == id) else { return };
            if mi.io.as_ref().is_none_or(|i| i.id != stream) {
                return;
            }
            if let Frame::Reject(r) = &f {
                tracing::info!(partition = p, migration = id, "the migration source refused the stream: {r:?}");
                if let Some(io) = mi.io.take() {
                    io.abort();
                }
                return;
            }
            let epoch = mi.epoch;
            let mut acts = Vec::new();
            match mi.stream.on_frame(f) {
                Ok(a) => acts.push(a),
                Err(_) => {
                    if let Some(io) = mi.io.take() {
                        io.abort();
                    }
                    return;
                }
            }
            while let Some(a) = mi.stream.next_action() {
                acts.push(a);
            }
            for a in acts {
                let ops = match a {
                    MemberAction::Snap(rec, _) => vec![Op::Put { db: rec.db, vtype: rec.vtype, expire_at: rec.expire_at, key: rec.key, value: rec.value }],
                    MemberAction::Apply(g) => g.ops.into_iter().filter(|o| !matches!(o, Op::FlushDb { .. } | Op::FlushSlot { .. } | Op::Noop)).collect(),
                    MemberAction::SyncDone(at) | MemberAction::Commit(at) => {
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
                let req = Request { db: 0, keys: ops_keys(&ops), kind: ReqKind::Keyed, origin: Origin::Client };
                submits.push((req, ReplJob { work: ReplWork::Own { ops }, then: Then::MigInApply { id, epoch, item } }));
            }
        }
        for (req, rj) in submits {
            self.submit_repl(cw, p, req, rj);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn mig_in_applied(&mut self, cw: &mut ClusterWorker, p: u16, id: u64, epoch: u64, item: u64, wrote: Option<Seq>, ok: bool) {
        let Some(pr) = cw.part(p) else { return };
        let Some(mi) = pr.mig_in.iter_mut().find(|m| m.id == id) else { return };
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
    }
}
