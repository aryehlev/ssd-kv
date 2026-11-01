//! Simulated block device (DESIGN §17.1, §25.3, §25.4; feature `sim`).
//!
//! `SimDevice` models the failure model of §17.1: a durable byte image plus a volatile
//! write-back set; FLUSH makes completed writes durable; `crash` (kill -9 model) keeps completed
//! writes and loses unsubmitted ones; `power_cut` drops unflushed writes and tears writes in
//! flight at byte, sector or prefix granularity, except `RWF_ATOMIC` writes no larger than the
//! atomic unit, which are all-or-nothing. Deterministic under its seed.
//!
//! It keeps only the durable image and the volatile write-back set in RAM; it records no
//! per-write history (LC-33). Used by `kv-sim` and by `tests/crash/faultdev`.
//!
//! Model details:
//! * Requests are validated like the kernel would ([`IoReq::check`]): misaligned or
//!   out-of-range requests, and `RWF_ATOMIC` writes that are not a power of two between `lba`
//!   and the atomic unit at an offset aligned to their length, fail with `-EINVAL`.
//! * Submitted requests complete in a seeded random order when [`BlockDevice::poll`] runs (at
//!   most [`SimDevice::set_max_completions`] per poll). Completed writes enter the volatile set
//!   per `lba` sector; `RWF_DSYNC` writes and every write of a write-through device are durable
//!   at completion.
//! * A flush makes durable every volatile sector whose newest content completed before the
//!   flush was submitted. A sector rewritten after that keeps only its newest content, which
//!   this flush does not cover: stricter than a real cache, and never relevant to a correct
//!   writer (E1: no rewrite before the previous image is durable).
//! * Reads return the durable image overlaid by the volatile set.
//! * A discard completes like a write of zeros (content after a discard is unspecified; zeros
//!   are one admissible outcome).

use foldhash::fast::FixedState;
use hashbrown::HashMap;

use crate::io::{AlignedBuf, BlockDevice, DevInfo, IoDone, IoOp, IoReq, PageUpdateMode};
use crate::layout::PAGE;

/// How in-flight writes are torn by `crash` / `power_cut`.
///
/// * `None`: nothing is torn: at a power cut every write not yet durable is lost whole; at a
///   crash every submitted request completes.
/// * `Bytes`: each byte of a write that is not durable independently keeps its old value or
///   takes its new one (the strongest model, §17.1).
/// * `Sectors`: the same per `lba` sector.
/// * `Prefix`: a write persists a prefix of its sectors (volatile data: a prefix of every run
///   of consecutive volatile sectors).
///
/// `RWF_ATOMIC` writes within the atomic unit are never torn: they persist whole or not at all.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TearMode {
    None,
    Bytes,
    Sectors,
    Prefix,
}

/// Content of one volatile (completed, not yet flushed) sector.
struct VolSector {
    data: Box<[u8]>,
    /// Completion generation of the write that produced it.
    generation: u64,
    /// Atomic write group (0 = plain write): sectors of one atomic write persist together.
    atom: u64,
}

/// A submitted request and, for flushes, the completion generation it covers.
struct Submitted {
    req: IoReq,
    covers: u64,
}

/// SplitMix64: small, fast, deterministic.
#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (`n > 0`).
    fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next()) * u128::from(n)) >> 64) as u64
    }

    fn coin(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

pub struct SimDevice {
    pages: u64,
    lba: u32,
    atomic_unit: Option<u32>,
    rng: Rng,
    /// The durable byte image.
    durable: Vec<u8>,
    /// Completed but unflushed sector contents, by sector index.
    volatile: HashMap<u64, VolSector, FixedState>,
    /// Submitted, not yet completed.
    in_flight: Vec<Submitted>,
    done: Vec<IoDone>,
    generation: u64,
    next_atom: u64,
    write_through: bool,
    discard: bool,
    page_update: PageUpdateMode,
    max_per_poll: usize,
    error_ppm: u32,
}

impl SimDevice {
    /// A zeroed device of `pages` 4 KiB pages with sector size `lba` (512 or 4096) and, when
    /// `atomic_unit` is set, power-fail atomic writes of up to that many bytes.
    pub fn new(pages: u64, lba: u32, atomic_unit: Option<u32>, seed: u64) -> Self {
        let bytes = usize::try_from(pages * PAGE as u64).expect("simulated device fits in memory");
        Self::from_image(vec![0u8; bytes], lba, atomic_unit, seed)
    }

    /// A device whose durable content is `image` (a power-cut result served as a fresh device).
    pub fn from_image(image: Vec<u8>, lba: u32, atomic_unit: Option<u32>, seed: u64) -> Self {
        assert!(lba == 512 || lba == 4096, "lba must be 512 or 4096");
        assert!(image.len().is_multiple_of(PAGE), "device image must be whole pages");
        if let Some(u) = atomic_unit {
            assert!(u.is_power_of_two() && u >= lba, "atomic unit must be a power of two >= lba");
        }
        let page_update = if atomic_unit.is_some_and(|u| u >= PAGE as u32) { PageUpdateMode::Atomic } else { PageUpdateMode::TwoImage };
        SimDevice {
            pages: (image.len() / PAGE) as u64,
            lba,
            atomic_unit,
            rng: Rng(seed ^ 0x005E_ED0F_D15C),
            durable: image,
            volatile: HashMap::with_hasher(FixedState::default()),
            in_flight: Vec::new(),
            done: Vec::new(),
            generation: 0,
            next_atom: 1,
            write_through: false,
            discard: true,
            page_update,
            max_per_poll: usize::MAX,
            error_ppm: 0,
        }
    }

    /// Models a device without a volatile cache (every completed write is durable).
    pub fn with_write_through(mut self, on: bool) -> Self {
        self.write_through = on;
        self
    }

    /// Overrides the page-update mode reported by `info` (default: atomic when the atomic unit
    /// covers a 4 KiB page).
    pub fn set_page_update(&mut self, m: PageUpdateMode) {
        self.page_update = m;
    }

    /// Completes at most `n` requests per `poll` (at least one), so that a driver can stop
    /// between any two completions.
    pub fn set_max_completions(&mut self, n: usize) {
        self.max_per_poll = n.max(1);
    }

    /// Fails completing reads, writes and flushes with `-EIO` at `ppm` per million; a failed
    /// write or flush changes nothing.
    pub fn set_error_rate(&mut self, ppm: u32) {
        self.error_ppm = ppm.min(1_000_000);
    }

    /// Submitted requests not completed yet.
    pub fn pending(&self) -> usize {
        self.in_flight.len()
    }

    /// Sectors holding completed but not yet durable data.
    pub fn volatile_sectors(&self) -> usize {
        self.volatile.len()
    }

    /// kill -9 model: requests already submitted complete (with a tear mode other than `None`
    /// each one completes, fails, or is left torn, chosen by the seed); completions are lost
    /// with the process; completed writes stay in the device's volatile cache.
    pub fn crash(&mut self, mode: TearMode) {
        let subs = core::mem::take(&mut self.in_flight);
        for s in subs {
            if mode == TearMode::None {
                self.complete(s, false);
                continue;
            }
            match self.rng.below(3) {
                0 => self.complete(s, false),
                1 => {}
                _ => self.tear_in_flight(s, mode, false),
            }
        }
        self.done.clear();
    }

    /// Power loss model: data not durable is resolved by `mode` (the volatile set first, then
    /// the in-flight writes on top of it); then the cache, the queue and the completions are
    /// empty. Afterwards the device serves exactly its durable image.
    pub fn power_cut(&mut self, mode: TearMode) {
        self.resolve_volatile(mode);
        let subs = core::mem::take(&mut self.in_flight);
        for s in subs {
            self.tear_in_flight(s, mode, true);
        }
        self.done.clear();
    }

    pub fn durable_image(&self) -> &[u8] {
        &self.durable
    }

    /// The durable image, consuming the device.
    pub fn into_image(self) -> Vec<u8> {
        self.durable
    }

    /// The content a read would return now (durable image overlaid by the volatile set).
    pub fn read_current(&self, offset: u64, out: &mut [u8]) {
        let lba = u64::from(self.lba);
        let mut pos = 0usize;
        while pos < out.len() {
            let abs = offset + pos as u64;
            let sector = abs / lba;
            let in_sec = (abs - sector * lba) as usize;
            let n = (self.lba as usize - in_sec).min(out.len() - pos);
            match self.volatile.get(&sector) {
                Some(v) => out[pos..pos + n].copy_from_slice(&v.data[in_sec..in_sec + n]),
                None => out[pos..pos + n].copy_from_slice(&self.durable[abs as usize..abs as usize + n]),
            }
            pos += n;
        }
    }

    /// Makes every volatile sector durable (as if a covering flush completed).
    pub fn flush_now(&mut self) {
        let mut sectors: Vec<u64> = self.volatile.keys().copied().collect();
        sectors.sort_unstable();
        for s in sectors {
            if let Some(v) = self.volatile.remove(&s) {
                self.write_durable(s * u64::from(self.lba), &v.data);
            }
        }
    }

    fn bytes(&self) -> u64 {
        self.pages * PAGE as u64
    }

    fn write_durable(&mut self, offset: u64, data: &[u8]) {
        let o = offset as usize;
        self.durable[o..o + data.len()].copy_from_slice(data);
    }

    /// Validation beyond the generic request rules: atomic writes.
    fn check(&self, r: &IoReq) -> Result<(), i32> {
        r.check(self.lba, self.bytes())?;
        if let IoOp::Write { atomic: true, .. } = r.op {
            let ok = self.atomic_unit.is_some_and(|u| {
                r.len.is_power_of_two() && r.len >= self.lba && r.len <= u && r.offset.is_multiple_of(u64::from(r.len))
            });
            if !ok {
                return Err(-libc::EINVAL);
            }
        }
        Ok(())
    }

    /// Applies a completed write of `data` at `offset`: durable now (`dsync` or write-through)
    /// or into the volatile set.
    fn apply_write(&mut self, offset: u64, data: &[u8], dsync: bool, atom: u64) {
        let lba = self.lba as usize;
        let first = offset / lba as u64;
        if dsync || self.write_through {
            self.write_durable(offset, data);
            for i in 0..(data.len() / lba) as u64 {
                self.volatile.remove(&(first + i));
            }
            return;
        }
        self.generation += 1;
        for (i, chunk) in data.chunks_exact(lba).enumerate() {
            let v = VolSector { data: chunk.into(), generation: self.generation, atom };
            self.volatile.insert(first + i as u64, v);
        }
    }

    fn flush_covered(&mut self, covers: u64) {
        let mut sectors: Vec<u64> = self.volatile.iter().filter(|(_, v)| v.generation <= covers).map(|(&s, _)| s).collect();
        sectors.sort_unstable();
        for s in sectors {
            if let Some(v) = self.volatile.remove(&s) {
                self.write_durable(s * u64::from(self.lba), &v.data);
            }
        }
    }

    fn atom_for(&mut self, atomic: bool) -> u64 {
        if !atomic {
            return 0;
        }
        let a = self.next_atom;
        self.next_atom += 1;
        a
    }

    /// Completes one request normally; queues its completion unless `quiet`.
    fn complete(&mut self, s: Submitted, quiet: bool) {
        let Submitted { mut req, covers } = s;
        let fail = self.error_ppm > 0 && self.rng.below(1_000_000) < u64::from(self.error_ppm);
        let res = if fail {
            Err(-libc::EIO)
        } else {
            let len = req.len as usize;
            match &mut req.op {
                IoOp::Read { buf } => {
                    self.read_current(req.offset, &mut buf.as_mut_slice()[..len]);
                    Ok(req.len)
                }
                IoOp::Write { buf, buf_off, dsync, atomic } => {
                    let atom = self.atom_for(*atomic);
                    let o = *buf_off as usize;
                    let (dsync, offset) = (*dsync, req.offset);
                    let data = buf.as_slice()[o..o + len].to_vec();
                    self.apply_write(offset, &data, dsync, atom);
                    Ok(req.len)
                }
                IoOp::Flush => {
                    self.flush_covered(covers);
                    Ok(0)
                }
                IoOp::Discard => {
                    let zeros = vec![0u8; len];
                    self.apply_write(req.offset, &zeros, false, 0);
                    Ok(req.len)
                }
            }
        };
        if !quiet {
            let tag = req.tag;
            self.done.push(IoDone { tag, res, buf: req.into_buf() });
        }
    }

    /// Resolves a write that was in flight at a crash (`to_durable == false`: the result lands
    /// in the volatile set) or a power cut (`true`: the result lands on media).
    fn tear_in_flight(&mut self, s: Submitted, mode: TearMode, to_durable: bool) {
        let req = s.req;
        let (buf, buf_off, atomic) = match req.op {
            IoOp::Write { buf, buf_off, atomic, .. } => (buf, buf_off as usize, atomic),
            // Reads change nothing; a flush in flight covers nothing for sure; a discard in
            // flight may or may not have happened: leave the old content.
            _ => return,
        };
        let len = req.len as usize;
        let new = &buf.as_slice()[buf_off..buf_off + len];
        let mut old = vec![0u8; len];
        if to_durable {
            old.copy_from_slice(&self.durable[req.offset as usize..req.offset as usize + len]);
        } else {
            self.read_current(req.offset, &mut old);
        }
        let mixed = if atomic {
            // RWF_ATOMIC within the verified unit: all or nothing.
            if mode != TearMode::None && self.rng.coin() { new.to_vec() } else { return }
        } else {
            match self.mix(mode, &old, new) {
                Some(m) => m,
                None => return,
            }
        };
        if to_durable {
            self.write_durable(req.offset, &mixed);
        } else {
            self.apply_write(req.offset, &mixed, false, 0);
        }
    }

    /// A torn image of `new` over `old` per `mode` (`None` = nothing persisted).
    fn mix(&mut self, mode: TearMode, old: &[u8], new: &[u8]) -> Option<Vec<u8>> {
        let lba = self.lba as usize;
        match mode {
            TearMode::None => None,
            TearMode::Bytes => {
                let mut out = old.to_vec();
                let mut bits = 0u64;
                for (i, (o, &n)) in out.iter_mut().zip(new).enumerate() {
                    if i % 64 == 0 {
                        bits = self.rng.next();
                    }
                    if bits & (1 << (i % 64)) != 0 {
                        *o = n;
                    }
                }
                Some(out)
            }
            TearMode::Sectors => {
                let mut out = old.to_vec();
                for (o, n) in out.chunks_exact_mut(lba).zip(new.chunks_exact(lba)) {
                    if self.rng.coin() {
                        o.copy_from_slice(n);
                    }
                }
                Some(out)
            }
            TearMode::Prefix => {
                let sectors = new.len() / lba;
                let keep = self.rng.below(sectors as u64 + 1) as usize * lba;
                let mut out = old.to_vec();
                out[..keep].copy_from_slice(&new[..keep]);
                Some(out)
            }
        }
    }

    /// Power cut, volatile part: every completed-but-unflushed sector is resolved by `mode`;
    /// sectors of one atomic write share one all-or-nothing decision.
    fn resolve_volatile(&mut self, mode: TearMode) {
        let mut sectors: Vec<u64> = self.volatile.keys().copied().collect();
        sectors.sort_unstable();
        let lba = u64::from(self.lba);
        let mut atoms: HashMap<u64, bool, FixedState> = HashMap::with_hasher(FixedState::default());
        let mut prefix_keep = true;
        let mut prev: Option<u64> = None;
        for s in sectors {
            let Some(v) = self.volatile.remove(&s) else { continue };
            let offset = s * lba;
            if v.atom != 0 {
                let keep = match atoms.get(&v.atom) {
                    Some(&k) => k,
                    None => {
                        let k = mode != TearMode::None && self.rng.coin();
                        atoms.insert(v.atom, k);
                        k
                    }
                };
                if keep {
                    self.write_durable(offset, &v.data);
                }
                prev = Some(s);
                continue;
            }
            match mode {
                TearMode::None => {}
                TearMode::Bytes | TearMode::Sectors => {
                    let o = offset as usize;
                    let old = self.durable[o..o + v.data.len()].to_vec();
                    if let Some(m) = self.mix(mode, &old, &v.data) {
                        self.write_durable(offset, &m);
                    }
                }
                TearMode::Prefix => {
                    // A new run of consecutive sectors starts a new prefix decision: keep
                    // sectors until the first dropped one.
                    if prev.is_none_or(|p| p + 1 != s) {
                        prefix_keep = true;
                    }
                    if prefix_keep && self.rng.below(8) == 0 {
                        prefix_keep = false;
                    }
                    if prefix_keep {
                        self.write_durable(offset, &v.data);
                    }
                }
            }
            prev = Some(s);
        }
    }
}

impl BlockDevice for SimDevice {
    fn info(&self) -> DevInfo {
        DevInfo {
            pages: self.pages,
            lba: self.lba,
            write_through: self.write_through,
            discard: self.discard,
            page_update: self.page_update,
            atomic_unit: self.atomic_unit.map(|u| (self.lba, u)),
        }
    }

    fn submit(&mut self, r: IoReq) {
        if let Err(e) = self.check(&r) {
            let tag = r.tag;
            self.done.push(IoDone { tag, res: Err(e), buf: r.into_buf() });
            return;
        }
        let covers = self.generation;
        self.in_flight.push(Submitted { req: r, covers });
    }

    fn poll(&mut self, out: &mut Vec<IoDone>, _wait: bool) {
        let n = self.in_flight.len().min(self.max_per_poll);
        for _ in 0..n {
            let i = self.rng.below(self.in_flight.len() as u64) as usize;
            let s = self.in_flight.swap_remove(i);
            self.complete(s, false);
        }
        out.append(&mut self.done);
    }
}

/// Convenience for tests and tools: a buffer filled with `byte`.
pub fn filled(len: usize, byte: u8) -> AlignedBuf {
    let mut b = AlignedBuf::zeroed(len);
    b.as_mut_slice().fill(byte);
    b
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::IoTag;

    fn tag(id: u64) -> IoTag {
        IoTag { partition: 0, id }
    }

    fn write(d: &mut SimDevice, id: u64, off: u64, len: usize, byte: u8) {
        d.submit(IoReq::write(tag(id), 0, off, filled(len, byte)));
    }

    fn drain(d: &mut SimDevice) -> Vec<IoDone> {
        let mut out = Vec::new();
        d.poll(&mut out, true);
        out
    }

    fn current(d: &SimDevice, off: u64, len: usize) -> Vec<u8> {
        let mut b = vec![0u8; len];
        d.read_current(off, &mut b);
        b
    }

    #[test]
    fn write_read_flush() {
        let mut d = SimDevice::new(4, 512, None, 1);
        write(&mut d, 1, 4096, 4096, 7);
        assert_eq!(d.pending(), 1);
        assert_eq!(current(&d, 4096, 4096), vec![0; 4096], "in-flight writes are not visible");
        let done = drain(&mut d);
        assert_eq!(done[0].res, Ok(4096));
        assert!(done[0].buf.is_some());
        assert_eq!(current(&d, 4096, 4096), vec![7; 4096]);
        assert_eq!(d.durable_image()[4096], 0, "completed, not flushed");
        assert_eq!(d.volatile_sectors(), 8);
        d.submit(IoReq::read(tag(2), 0, 4096, AlignedBuf::zeroed(4096)));
        let done = drain(&mut d);
        assert!(done[0].buf.as_ref().unwrap().iter().all(|&b| b == 7));
        d.submit(IoReq::flush(tag(3), 0));
        assert_eq!(drain(&mut d)[0].res, Ok(0));
        assert_eq!(d.volatile_sectors(), 0);
        assert!(d.durable_image()[4096..8192].iter().all(|&b| b == 7));
        // A power cut now loses nothing.
        d.power_cut(TearMode::Bytes);
        assert!(d.durable_image()[4096..8192].iter().all(|&b| b == 7));
    }

    #[test]
    fn flush_covers_only_writes_completed_before_it() {
        let mut d = SimDevice::new(4, 4096, None, 2);
        write(&mut d, 1, 0, 4096, 1);
        drain(&mut d);
        d.submit(IoReq::flush(tag(2), 0));
        write(&mut d, 3, 4096, 4096, 3); // in flight when the flush was submitted
        d.set_max_completions(1);
        let mut seen = Vec::new();
        while d.pending() > 0 {
            seen.extend(drain(&mut d).into_iter().map(|x| x.tag.id));
        }
        assert_eq!(seen.len(), 2);
        assert_eq!(d.durable_image()[0], 1);
        assert_eq!(d.durable_image()[4096], 0, "write 3 is not covered by flush 2");
        d.power_cut(TearMode::None);
        assert_eq!(d.durable_image()[4096], 0);
        assert_eq!(current(&d, 4096, 1), vec![0]);
    }

    #[test]
    fn dsync_and_write_through_are_durable_at_completion() {
        let mut d = SimDevice::new(2, 512, None, 3);
        write(&mut d, 1, 0, 512, 5);
        drain(&mut d);
        d.submit(IoReq::write_span(tag(2), 0, 0, filled(512, 6), 0, 512, true, false));
        drain(&mut d);
        assert_eq!(d.durable_image()[0], 6);
        assert_eq!(d.volatile_sectors(), 0, "the older volatile content is superseded");
        d.power_cut(TearMode::Bytes);
        assert_eq!(d.durable_image()[0], 6);

        let mut d = SimDevice::new(2, 512, None, 3).with_write_through(true);
        assert!(d.info().write_through);
        write(&mut d, 1, 512, 512, 9);
        drain(&mut d);
        d.power_cut(TearMode::None);
        assert_eq!(d.durable_image()[512], 9);
    }

    #[test]
    fn power_cut_drops_unflushed_and_tears_in_flight() {
        for (seed, mode) in [(1, TearMode::None), (2, TearMode::Bytes), (3, TearMode::Sectors), (4, TearMode::Prefix)] {
            let mut d = SimDevice::new(8, 512, None, seed);
            write(&mut d, 1, 0, 8192, 0x11);
            drain(&mut d);
            d.submit(IoReq::flush(tag(2), 0));
            drain(&mut d);
            write(&mut d, 3, 0, 8192, 0x22); // completed, unflushed
            drain(&mut d);
            write(&mut d, 4, 8192, 8192, 0x33); // in flight
            d.power_cut(mode);
            assert_eq!(d.pending(), 0);
            assert_eq!(d.volatile_sectors(), 0);
            let img = d.durable_image();
            // Every byte is old or new, never anything else.
            assert!(img[..8192].iter().all(|&b| b == 0x11 || b == 0x22));
            assert!(img[8192..16384].iter().all(|&b| b == 0 || b == 0x33));
            match mode {
                TearMode::None => {
                    assert!(img[..8192].iter().all(|&b| b == 0x11));
                    assert!(img[8192..16384].iter().all(|&b| b == 0));
                }
                TearMode::Sectors => {
                    for s in img[..16384].chunks(512) {
                        assert!(s.iter().all(|&b| b == s[0]), "sector granularity");
                    }
                }
                TearMode::Prefix => {
                    let new: Vec<bool> = img[8192..16384].chunks(512).map(|s| s[0] == 0x33).collect();
                    assert!(new.windows(2).all(|w| w[0] || !w[1]), "a prefix of the write persists");
                }
                TearMode::Bytes => {}
            }
        }
    }

    #[test]
    fn byte_tearing_mixes_bytes() {
        let mut d = SimDevice::new(1, 512, None, 99);
        write(&mut d, 1, 0, 4096, 0xFF);
        d.power_cut(TearMode::Bytes);
        let n = d.durable_image().iter().filter(|&&b| b == 0xFF).count();
        assert!(n > 1000 && n < 3000, "{n} of 4096 bytes new");
    }

    #[test]
    fn atomic_writes_are_never_torn() {
        for seed in 0..40 {
            let mut d = SimDevice::new(4, 512, Some(4096), seed);
            assert_eq!(d.info().page_update, PageUpdateMode::Atomic);
            assert_eq!(d.info().atomic_unit, Some((512, 4096)));
            d.submit(IoReq::write_span(tag(1), 0, 0, filled(4096, 0xAA), 0, 4096, false, true));
            if seed.is_multiple_of(2) {
                drain(&mut d); // volatile, unflushed
            }
            d.submit(IoReq::write_span(tag(2), 0, 4096, filled(4096, 0xBB), 0, 4096, false, true));
            d.power_cut(if seed.is_multiple_of(3) { TearMode::Sectors } else { TearMode::Bytes });
            let img = d.durable_image();
            for (p, v) in [(0usize, 0xAAu8), (4096, 0xBB)] {
                let page = &img[p..p + 4096];
                assert!(page.iter().all(|&b| b == 0) || page.iter().all(|&b| b == v), "seed {seed}");
            }
        }
        // Atomic writes outside the unit or misaligned are refused.
        let mut d = SimDevice::new(4, 512, Some(4096), 0);
        d.submit(IoReq::write_span(tag(1), 0, 0, filled(8192, 1), 0, 8192, false, true));
        d.submit(IoReq::write_span(tag(2), 0, 512, filled(4096, 1), 0, 4096, false, true));
        d.submit(IoReq::write_span(tag(3), 0, 0, filled(1024, 1), 0, 1024, false, true));
        let res: Vec<_> = drain(&mut d).into_iter().map(|x| (x.tag.id, x.res)).collect();
        assert_eq!(res, vec![(1, Err(-libc::EINVAL)), (2, Err(-libc::EINVAL)), (3, Ok(1024))]);
        let mut d = SimDevice::new(1, 4096, None, 0);
        assert_eq!(d.info().page_update, PageUpdateMode::TwoImage);
        d.submit(IoReq::write_span(tag(1), 0, 0, filled(4096, 1), 0, 4096, false, true));
        assert_eq!(drain(&mut d)[0].res, Err(-libc::EINVAL));
    }

    #[test]
    fn crash_keeps_completed_writes() {
        let mut d = SimDevice::new(2, 4096, None, 5);
        write(&mut d, 1, 0, 4096, 1);
        drain(&mut d);
        write(&mut d, 2, 4096, 4096, 2);
        d.crash(TearMode::None);
        assert_eq!(d.pending(), 0);
        assert!(drain(&mut d).is_empty(), "completions die with the process");
        assert_eq!(current(&d, 0, 1), vec![1]);
        assert_eq!(current(&d, 4096, 1), vec![2]);
        assert_eq!(d.durable_image()[0], 0, "still only in the device cache");
        // With tearing, a crash leaves each in-flight write whole, absent or torn.
        let mut d = SimDevice::new(1, 512, None, 6);
        write(&mut d, 1, 0, 4096, 3);
        d.crash(TearMode::Sectors);
        let b = current(&d, 0, 4096);
        assert!(b.iter().all(|&x| x == 0 || x == 3));
    }

    #[test]
    fn deterministic_under_seed() {
        let run = |seed| {
            let mut d = SimDevice::new(16, 512, Some(4096), seed);
            d.set_max_completions(3);
            for i in 0..12u64 {
                write(&mut d, i, (i * 3 % 16) * 4096, 4096, i as u8 + 1);
                if i % 4 == 3 {
                    d.submit(IoReq::flush(tag(100 + i), 0));
                }
                drain(&mut d);
            }
            write(&mut d, 50, 0, 8192, 0xEE);
            d.power_cut(TearMode::Bytes);
            d.into_image()
        };
        assert_eq!(run(7), run(7));
        assert_ne!(run(7), run(8));
    }

    #[test]
    fn rejects_bad_requests_and_injects_errors() {
        let mut d = SimDevice::new(1, 512, None, 0);
        d.submit(IoReq::write(tag(1), 0, 4096, filled(512, 1))); // past the end
        d.submit(IoReq::write(tag(2), 0, 100, filled(512, 1))); // misaligned
        let res: Vec<_> = drain(&mut d).into_iter().map(|x| x.res).collect();
        assert_eq!(res, vec![Err(-libc::EINVAL), Err(-libc::EINVAL)]);
        d.set_error_rate(1_000_000);
        write(&mut d, 3, 0, 512, 1);
        assert_eq!(drain(&mut d)[0].res, Err(-libc::EIO));
        assert_eq!(current(&d, 0, 1), vec![0]);
        d.set_error_rate(0);
        d.submit(IoReq::discard(tag(4), 0, 0, 512));
        assert_eq!(drain(&mut d)[0].res, Ok(512));
    }

    #[test]
    fn from_image_serves_the_image() {
        let mut img = vec![0u8; 8192];
        img[100] = 42;
        let mut d = SimDevice::from_image(img, 512, None, 0);
        assert_eq!(d.info().pages, 2);
        d.submit(IoReq::read(tag(1), 0, 0, AlignedBuf::zeroed(512)));
        assert_eq!(drain(&mut d)[0].buf.as_ref().unwrap()[100], 42);
        write(&mut d, 2, 0, 512, 1);
        drain(&mut d);
        d.flush_now();
        assert_eq!(d.durable_image()[100], 1);
    }
}
