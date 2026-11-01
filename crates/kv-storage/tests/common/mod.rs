//! Shared harness of the kv-storage integration tests: a RAM block device with the kill -9
//! failure model, a synchronous command driver over `LocalEngine`, and a reference model of the
//! acknowledged state (DESIGN §25.3 I1-I3).
#![allow(dead_code)]


use kv_common::{Access, Now};
use kv_index::VType;
use kv_storage::engine::{EngineCfg, LocalEngine};
use kv_storage::io::{AlignedBuf, BlockDevice, DevInfo, IoDone, IoOp, IoReq, PageUpdateMode};
use kv_storage::keyspace::{KeyMeta, StoreError};
use kv_storage::partition::{ExecOut, Origin, ReqKind, Request};
use kv_storage::ticket::{TicketId, TicketState};
use smallvec::SmallVec;

/// A RAM device: requests take effect when they complete (at `poll`, a seeded number per
/// poll and in a seeded order); a crash (kill -9) drops every request not completed yet.
/// Completed writes are durable (the test models a write-through cache).
pub struct RamDev {
    pub data: Vec<u8>,
    lba: u32,
    pending: Vec<IoReq>,
    rng: u64,
    max_per_poll: usize,
    atomic: Option<(u32, u32)>,
}

impl RamDev {
    pub fn new(pages: u64, lba: u32, seed: u64) -> Self {
        RamDev { data: vec![0u8; pages as usize * 4096], lba, pending: Vec::new(), rng: seed | 1, max_per_poll: usize::MAX, atomic: None }
    }

    pub fn from_image(data: Vec<u8>, lba: u32, seed: u64) -> Self {
        RamDev { data, lba, pending: Vec::new(), rng: seed | 1, max_per_poll: usize::MAX, atomic: None }
    }

    /// Reports a verified atomic write unit (atomic page-update mode).
    pub fn with_atomic(mut self, unit: (u32, u32)) -> Self {
        self.atomic = Some(unit);
        self
    }

    pub fn set_max_completions(&mut self, n: usize) {
        self.max_per_poll = n.max(1);
    }

    /// kill -9: requests not completed are lost.
    pub fn crash(&mut self) {
        self.pending.clear();
    }

    fn next(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }
}

impl BlockDevice for RamDev {
    fn info(&self) -> DevInfo {
        DevInfo {
            pages: (self.data.len() / 4096) as u64,
            lba: self.lba,
            write_through: true,
            discard: false,
            page_update: if self.atomic.is_some() { PageUpdateMode::Atomic } else { PageUpdateMode::TwoImage },
            atomic_unit: self.atomic,
        }
    }

    fn submit(&mut self, r: IoReq) {
        self.pending.push(r);
    }

    fn poll(&mut self, out: &mut Vec<IoDone>, wait: bool) {
        let mut n = if wait { self.pending.len() } else { self.max_per_poll.min(self.pending.len()) };
        while n > 0 && !self.pending.is_empty() {
            n -= 1;
            let i = (self.next() % self.pending.len() as u64) as usize;
            let mut r = self.pending.swap_remove(i);
            let res = match r.check(self.lba, self.data.len() as u64) {
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
            out.push(IoDone { tag, res, buf: r.into_buf() });
        }
    }
}

/// Engine configuration for tests: small cache, default rates.
pub fn cfg() -> EngineCfg {
    let mut c = EngineCfg::defaults(Vec::new());
    c.cache_bytes = 8 << 20;
    c
}

/// A keyed request.
pub fn keyed(db: u8, keys: &[&[u8]], access: Access) -> Request {
    Request { db, keys: keys.iter().map(|k| (Box::<[u8]>::from(*k), access)).collect(), kind: ReqKind::Keyed, origin: Origin::Client }
}

/// Command helpers over an engine.
pub struct Db<D: BlockDevice> {
    pub eng: LocalEngine<D>,
    pub now_ms: u64,
}

impl<D: BlockDevice> Db<D> {
    pub fn new(mut eng: LocalEngine<D>, now_ms: u64) -> Self {
        eng.set_now(Now { mono_ns: now_ms * 1_000_000, boot_ns: now_ms * 1_000_000, unix_ms: now_ms });
        Db { eng, now_ms }
    }

    pub fn advance(&mut self, ms: u64) {
        self.now_ms += ms;
        let n = self.now_ms;
        self.eng.set_now(Now { mono_ns: n * 1_000_000, boot_ns: n * 1_000_000, unix_ms: n });
    }

    fn part(&self, key: &[u8]) -> u16 {
        self.eng.partition_of(key)
    }

    pub fn set(&mut self, db: u8, key: &[u8], val: &[u8], expire: Option<u64>) -> Result<Option<TicketId>, StoreError> {
        let p = self.part(key);
        let (_, out) = self.eng.run(p, keyed(db, &[key], Access::Write), &mut |ks| {
            ks.select(db);
            ks.put(key, val, VType::String, expire)
        })?;
        Ok(out.ticket)
    }

    pub fn get(&mut self, db: u8, key: &[u8]) -> Result<Option<(KeyMeta, Vec<u8>)>, StoreError> {
        let p = self.part(key);
        let (v, _) = self.eng.run(p, keyed(db, &[key], Access::Read), &mut |ks| {
            ks.select(db);
            Ok(ks.get(key)?.map(|(m, v)| (m, v.to_vec())))
        })?;
        Ok(v)
    }

    pub fn del(&mut self, db: u8, key: &[u8]) -> Result<(bool, Option<TicketId>), StoreError> {
        let p = self.part(key);
        let (v, out) = self.eng.run(p, keyed(db, &[key], Access::Write), &mut |ks| {
            ks.select(db);
            ks.delete(key)
        })?;
        Ok((v, out.ticket))
    }

    pub fn expire(&mut self, db: u8, key: &[u8], at: Option<u64>) -> Result<(bool, Option<TicketId>), StoreError> {
        let p = self.part(key);
        let (v, out) = self.eng.run(p, keyed(db, &[key], Access::ReadWrite), &mut |ks| {
            ks.select(db);
            ks.set_expire(key, at)
        })?;
        Ok((v, out.ticket))
    }

    pub fn append(&mut self, db: u8, key: &[u8], data: &[u8]) -> Result<(u64, Option<TicketId>), StoreError> {
        let p = self.part(key);
        let (v, out) = self.eng.run(p, keyed(db, &[key], Access::ReadWrite), &mut |ks| {
            ks.select(db);
            ks.append(key, data)
        })?;
        Ok((v, out.ticket))
    }

    /// MSET of keys that share a partition (all keys must map to the same partition).
    pub fn mset(&mut self, db: u8, kvs: &[(&[u8], &[u8])]) -> Result<Option<TicketId>, StoreError> {
        let p = self.part(kvs[0].0);
        let keys: Vec<&[u8]> = kvs.iter().map(|(k, _)| *k).collect();
        let (_, out) = self.eng.run(p, keyed(db, &keys, Access::Write), &mut |ks| {
            ks.select(db);
            for (k, v) in kvs {
                ks.put(k, v, VType::String, None)?;
            }
            Ok(())
        })?;
        Ok(out.ticket)
    }

    pub fn flushdb(&mut self, db: Option<u8>) -> Vec<(u16, Option<TicketId>)> {
        let mut out = Vec::new();
        for p in 0..self.eng.partitions() {
            let req = Request { db: db.unwrap_or(0), keys: SmallVec::new(), kind: ReqKind::FlushDb { db }, origin: Origin::Client };
            let (_, o): ((), ExecOut) = self.eng.run(p, req, &mut |_| Ok(())).expect("flush");
            out.push((p, o.ticket));
        }
        out
    }

    /// Waits until `t` of the key's partition reached `want`.
    pub fn wait(&mut self, key: &[u8], t: Option<TicketId>, want: TicketState) -> TicketState {
        match t {
            None => TicketState::Durable,
            Some(t) => {
                let p = self.part(key);
                self.eng.wait_ticket(p, t, want)
            }
        }
    }

    /// Every key of `db` (SCAN to the end, COUNT 10).
    pub fn scan_all(&mut self, db: u8) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for p in 0..self.eng.partitions() {
            let mut cur = kv_index::cursor::encode(p, 0, 0);
            loop {
                let req = Request { db, keys: SmallVec::new(), kind: ReqKind::ScanStep { db, cursor: cur, count: 10, vtype: None }, origin: Origin::Client };
                let (next, ks) = self.eng.run_scan(p, req, &|_| true).expect("scan");
                keys.extend(ks.into_iter().map(|k| k.to_vec()));
                let (np, _, _) = kv_index::cursor::decode(next);
                if np != p {
                    break;
                }
                cur = next;
            }
        }
        keys.sort();
        keys
    }
}

/// A key's acknowledged state and the states of later unacknowledged operations.
pub type KeyStates = (Option<Vec<u8>>, Vec<Option<Vec<u8>>>);

/// Valid copies of each key: `(db, key) -> [(page, seq, small)]`.
pub type CopyMap = hashbrown::HashMap<(u8, Vec<u8>), Vec<(u64, u64, bool)>>;

/// The reference model: per key, the acknowledged value and the values of operations not
/// acknowledged yet (any of which may survive a crash, I1).
#[derive(Default, Clone)]
pub struct Model {
    /// key -> (acked state, states produced by later unacked ops)
    pub keys: hashbrown::HashMap<Vec<u8>, KeyStates>,
}

impl Model {
    /// An operation was executed (not acknowledged yet).
    pub fn executed(&mut self, key: &[u8], v: Option<Vec<u8>>) {
        self.keys.entry(key.to_vec()).or_insert((None, Vec::new())).1.push(v);
    }

    /// Every executed operation of `key` so far is acknowledged.
    pub fn acked(&mut self, key: &[u8]) {
        if let Some(e) = self.keys.get_mut(key)
            && let Some(last) = e.1.pop()
        {
            e.0 = last;
            e.1.clear();
        }
    }

    /// After a crash the key must hold its acknowledged state or one of the later states (I1,
    /// I2: acknowledged deletes never come back).
    pub fn check(&self, key: &[u8], got: Option<&[u8]>) -> Result<(), String> {
        let (acked, later) = self.keys.get(key).cloned().unwrap_or((None, Vec::new()));
        let ok = acked.as_deref() == got || later.iter().any(|v| v.as_deref() == got);
        if ok { Ok(()) } else { Err(format!("key of {} bytes: unexpected state ({} bytes)", key.len(), got.map_or(0, |g| g.len()))) }
    }

    /// The model after a crash: whatever the store holds now becomes the acknowledged state.
    pub fn resolve(&mut self, key: &[u8], got: Option<Vec<u8>>) {
        self.keys.insert(key.to_vec(), (got, Vec::new()));
    }
}

/// A deterministic value of `len` bytes.
pub fn value(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// A buffer with `data`.
pub fn buf(data: &[u8]) -> AlignedBuf {
    AlignedBuf::from_slice(data)
}

/// Counts the valid copies of every key on a device image (SMALL records and VALUE/CHUNKMAP
/// extents; checksums verified, the location bound): `(db, key) -> copies`. Dead records are
/// not filtered (callers avoid FLUSH and multi-key txns).
pub fn count_copies(image: &[u8], dev: u8, boot: &kv_storage::Boot) -> hashbrown::HashMap<(u8, Vec<u8>), usize> {
    use kv_storage::extent;
    use kv_storage::layout::{AG_PAGES, PAGE, PageId};
    use kv_storage::page::{PageParse, parse_small_page};
    let crc = boot.record_crc();
    let l = boot.layouts()[usize::from(dev)];
    let mut out: hashbrown::HashMap<(u8, Vec<u8>), usize> = hashbrown::HashMap::new();
    let mut views = Vec::new();
    let first = l.ag_area_page;
    let end = first + u64::from(l.n_ags) * u64::from(AG_PAGES);
    let mut page = first;
    while page < end {
        let at = page as usize * PAGE;
        let buf: &[u8; PAGE] = image[at..at + PAGE].try_into().unwrap();
        let id = PageId((u64::from(dev) << 32) | page);
        let part = u16::from(buf[5]);
        if parse_small_page(buf, id, part, crc, &mut views) == PageParse::Ok {
            for v in &views {
                *out.entry((v.hdr.db, v.key.to_vec())).or_insert(0) += 1;
            }
            page += 1;
            continue;
        }
        if let Some(h) = extent::parse_extent_header(buf, id) {
            let n = h.n_pages as usize;
            if at + n * PAGE <= image.len() && h.kind != extent::EXT_KIND_CHUNK && extent::verify_extent(&image[at..at + n * PAGE], &h, crc) {
                let kl = h.key_len as usize;
                let key = image[at + 80..at + 80 + kl].to_vec();
                *out.entry((h.db, key)).or_insert(0) += 1;
                page += n as u64;
                continue;
            }
        }
        page += 1;
    }
    out
}

/// Spearman's rank correlation of `xs` and `ys`.
pub fn spearman(xs: &[f64], ys: &[f64]) -> f64 {
    fn ranks(v: &[f64]) -> Vec<f64> {
        let mut idx: Vec<usize> = (0..v.len()).collect();
        idx.sort_by(|&a, &b| v[a].partial_cmp(&v[b]).unwrap());
        let mut r = vec![0.0; v.len()];
        let mut i = 0;
        while i < idx.len() {
            let mut j = i;
            while j + 1 < idx.len() && v[idx[j + 1]] == v[idx[i]] {
                j += 1;
            }
            let avg = (i + j) as f64 / 2.0;
            for k in i..=j {
                r[idx[k]] = avg;
            }
            i = j + 1;
        }
        r
    }
    let (a, b) = (ranks(xs), ranks(ys));
    let n = a.len() as f64;
    let (ma, mb) = (a.iter().sum::<f64>() / n, b.iter().sum::<f64>() / n);
    let cov: f64 = a.iter().zip(&b).map(|(x, y)| (x - ma) * (y - mb)).sum();
    let va: f64 = a.iter().map(|x| (x - ma).powi(2)).sum();
    let vb: f64 = b.iter().map(|y| (y - mb).powi(2)).sum();
    cov / (va.sqrt() * vb.sqrt())
}

/// Every valid copy on an image: `(db, key) -> [(page, seq, small)]`.
pub fn copy_locations(image: &[u8], dev: u8, boot: &kv_storage::Boot) -> CopyMap {
    use kv_storage::extent;
    use kv_storage::layout::{AG_PAGES, PAGE, PageId};
    use kv_storage::page::{PageParse, parse_small_page};
    let crc = boot.record_crc();
    let l = boot.layouts()[usize::from(dev)];
    let mut out: CopyMap = hashbrown::HashMap::new();
    let mut views = Vec::new();
    let first = l.ag_area_page;
    let end = first + u64::from(l.n_ags) * u64::from(AG_PAGES);
    let mut page = first;
    while page < end {
        let at = page as usize * PAGE;
        let buf: &[u8; PAGE] = image[at..at + PAGE].try_into().unwrap();
        let id = PageId((u64::from(dev) << 32) | page);
        if parse_small_page(buf, id, u16::from(buf[5]), crc, &mut views) == PageParse::Ok {
            for v in &views {
                out.entry((v.hdr.db, v.key.to_vec())).or_default().push((page, v.hdr.seq.0, true));
            }
            page += 1;
            continue;
        }
        if let Some(h) = extent::parse_extent_header(buf, id) {
            let n = h.n_pages as usize;
            if at + n * PAGE <= image.len() && h.kind != extent::EXT_KIND_CHUNK && extent::verify_extent(&image[at..at + n * PAGE], &h, crc) {
                let kl = h.key_len as usize;
                out.entry((h.db, image[at + 80..at + 80 + kl].to_vec())).or_default().push((page, h.seq.0, false));
                page += n as u64;
                continue;
            }
        }
        page += 1;
    }
    out
}

/// Spearman correlation pooled over groups: each group's values are ranked within the group
/// (seqs are per partition, so they are only comparable inside one), then the centred,
/// normalised ranks of every group are pooled into one Pearson correlation.
pub fn pooled_spearman(groups: &[(Vec<f64>, Vec<f64>)]) -> f64 {
    fn ranks(v: &[f64]) -> Vec<f64> {
        let mut idx: Vec<usize> = (0..v.len()).collect();
        idx.sort_by(|&a, &b| v[a].partial_cmp(&v[b]).unwrap());
        let mut r = vec![0.0; v.len()];
        let n = v.len().max(2) as f64 - 1.0;
        let mut i = 0;
        while i < idx.len() {
            let mut j = i;
            while j + 1 < idx.len() && v[idx[j + 1]] == v[idx[i]] {
                j += 1;
            }
            let avg = (i + j) as f64 / 2.0;
            for k in i..=j {
                r[idx[k]] = avg / n - 0.5;
            }
            i = j + 1;
        }
        r
    }
    let (mut a, mut b) = (Vec::new(), Vec::new());
    for (x, y) in groups {
        if x.len() < 2 {
            continue;
        }
        a.extend(ranks(x));
        b.extend(ranks(y));
    }
    let cov: f64 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
    let va: f64 = a.iter().map(|x| x * x).sum();
    let vb: f64 = b.iter().map(|y| y * y).sum();
    cov / (va.sqrt() * vb.sqrt())
}
