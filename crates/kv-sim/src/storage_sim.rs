//! Storage simulation (DESIGN §17, §25.3).
//!
//! N partitions of one engine ([`LocalEngine`]) on `SimDevice`s (durable + volatile layers,
//! FLUSH semantics, lost unflushed writes, byte/sector/prefix tearing; atomic-unit mode), both
//! page-update modes, write-through and write-back device caches, durability modes, lba
//! 512/4096. A single-threaded driver with a seeded RNG submits command mixes (SET variants
//! incl. TTLs, GET, DEL, GETDEL, EXPIRE/PERSIST, RENAME, COPY, MSET, MSETNX, APPEND/SETRANGE on
//! every size class up to chunkmaps of several MiB, FLUSHDB, FLUSHALL, barriers, SCAN) through
//! the public partition API (submit / ready / exec / pump), pipelined: many requests are in
//! flight at once. Device completions arrive late and out of order ([`SimDev`]).
//!
//! Checks:
//! * at exec time, every command's observations (values read, existence, lengths) equal the
//!   reference model's ([`crate::model`]); per-key order is the execution order;
//! * after every crash (kill -9 or power cut, at any I/O completion boundary, with or without
//!   torn writes) and every clean restart: I1, I2 and I3 per key against the acknowledged
//!   ticket states, I4 for multi-key writes, SCAN equals the set of live keys, I10;
//! * I15 (liveness): at every quiesce point and after every recovery, ticks with no new input
//!   bring every accepted request to a terminal state (reply released: success or error)
//!   within `hang_ticks` ticks; then every executed write reaches `Durable` and every partition
//!   becomes quiescent within `settle_ticks`. A request outstanding beyond the bound is a HANG,
//!   reported with its kind, its wait reason (driver phase, ticket state, partition queues from
//!   `PartitionStore::diag_deep`) and the seed.
//!
//! A run is a pure function of its configuration and its step list ([`Step`]): failing cases
//! are written as replayable case files (configuration + steps; no device trace, no payloads:
//! values are regenerated from step indices) and shrunk by [`minimize`].

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::rc::Rc;

use kv_common::{Access, Now};
use kv_index::VType;
use kv_storage::engine::{EngineCfg, LocalEngine, format_devices};
use kv_storage::io::{BlockDevice, DevInfo, IoDone, IoReq, PageUpdateMode};
use kv_storage::keyspace::{Keyspace, StoreError};
use kv_storage::partition::{BgRates, Durability, ExecStatus, Origin, ReqId, ReqKind, Request};
use kv_storage::recovery::RecoveryMode;
use kv_storage::sim::{SimDevice, TearMode};
use kv_storage::ticket::{TicketId, TicketState};

use crate::model::{Ack, KeyRef, Loss, RefModel, State};

/// Simulated wall clock at the first start (2027).
pub const T0: u64 = 1_800_000_000_000;

// ---------------------------------------------------------------------------------------------
// Deterministic randomness

/// SplitMix64.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed ^ 0x6A09_E667_F3BC_C908)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (0 when `n == 0`).
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// Uniform in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi.saturating_sub(lo) + 1)
    }

    pub fn chance(&mut self, pct: u32) -> bool {
        self.below(100) < u64::from(pct)
    }

    pub fn shuffle<T>(&mut self, xs: &mut [T]) {
        for i in (1..xs.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            xs.swap(i, j);
        }
    }

    /// Index chosen by `weights`.
    pub fn weighted(&mut self, weights: &[u32]) -> usize {
        let total: u64 = weights.iter().map(|&w| u64::from(w)).sum();
        let mut x = self.below(total.max(1));
        for (i, &w) in weights.iter().enumerate() {
            if x < u64::from(w) {
                return i;
            }
            x -= u64::from(w);
        }
        weights.len() - 1
    }
}

/// The bytes written by step `uid` (member `j` of a multi-key write): the ids lead, so that
/// distinct writes never share a fingerprint; the rest is pseudo-random.
pub fn value(uid: u64, j: u32, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let tag = (uid << 8) | u64::from(j & 0xFF);
    out.extend_from_slice(&tag.to_le_bytes()[..len.min(8)]);
    let mut r = Rng::new(tag.wrapping_mul(0xD6E8_FEB8_6659_FD93));
    while out.len() < len {
        let w = r.next_u64().to_le_bytes();
        let n = (len - out.len()).min(8);
        out.extend_from_slice(&w[..n]);
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Configuration

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PageMode {
    Atomic,
    TwoImage,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CacheMode {
    WriteThrough,
    WriteBack,
}

/// The value-size and command mix of a run.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Profile {
    /// Every size class, every command.
    Mixed,
    /// Small records only (dense pages, moves across the 2 KiB boundary).
    Small,
    /// Extents and chunkmaps of several MiB.
    Large,
    /// A device near its fill limit: out-of-space, fragmentation, frees.
    Full,
    /// Sizes changing across classes on few keys; frequent deletes and FLUSH.
    Churn,
}

impl Profile {
    fn name(self) -> &'static str {
        match self {
            Profile::Mixed => "mixed",
            Profile::Small => "small",
            Profile::Large => "large",
            Profile::Full => "full",
            Profile::Churn => "churn",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "mixed" => Profile::Mixed,
            "small" => Profile::Small,
            "large" => Profile::Large,
            "full" => Profile::Full,
            "churn" => Profile::Churn,
            _ => return None,
        })
    }
}

/// Restricts the parameters drawn from a seed (CLI `--mode`, `--cache`, `--lba`).
#[derive(Clone, Debug, Default)]
pub struct Filter {
    pub mode: Option<PageMode>,
    pub cache: Option<CacheMode>,
    pub lba: Option<u32>,
    /// Small runs for the debug-build smoke test.
    pub smoke: bool,
    /// Multiplies the number of steps (0 = 1).
    pub scale: f32,
    /// `key=value` overrides of drawn parameters (CLI `--set`).
    pub overrides: Vec<(String, String)>,
}

/// One simulation run's parameters (all drawn from `seed` unless a case file names them).
#[derive(Clone, Debug, PartialEq)]
pub struct StorageSimCfg {
    pub seed: u64,
    pub partitions: u16,
    pub devices: u8,
    /// AGs per device.
    pub ags: u32,
    pub lba: u32,
    pub page_mode: PageMode,
    /// The device's verified atomic write unit (bytes).
    pub atomic_unit: Option<u32>,
    pub cache: CacheMode,
    /// `flush-method dsync`: writes carry RWF_DSYNC.
    pub dsync: bool,
    pub durability: Durability,
    pub cache_bytes: usize,
    /// `max-storage-fill` in percent.
    pub fill_pct: u32,
    /// Format with snapshot regions (clean restarts take the snapshot path).
    pub snapshot: bool,
    pub keys: u32,
    /// Hash-tag groups (multi-key commands draw their keys from one group).
    pub groups: u32,
    pub dbs: u8,
    pub profile: Profile,
    pub steps: u32,
    pub rounds: u32,
    /// Per mille of steps that drive ticks (lower = deeper pipelines).
    pub tick_permille: u32,
    /// Completions held back up to this many device polls.
    pub delay_max: u32,
    /// Percent of completions held back.
    pub late_pct: u32,
    /// Percent of polls that leave the device idle.
    pub skip_pct: u32,
    /// Completions per device poll (0 = every one that is due).
    pub per_poll: u32,
    /// Execute ready requests in a shuffled order.
    pub shuffle: bool,
    /// Clock advance per tick (ms).
    pub tick_ms: u32,
    /// I15 bound: ticks for every accepted request to reach a terminal state.
    pub hang_ticks: u32,
    /// Ticks for every executed write to become durable and every partition quiescent.
    pub settle_ticks: u32,
    /// Injected device errors (per million requests; 0 = none): the request fails with EIO
    /// and changes nothing. Errors are off during recovery and verification.
    pub error_ppm: u32,
    /// Masks for known failure triggers (CLI `--set`), so that campaigns can look past them:
    /// no FLUSHDB/FLUSHALL commands,
    pub no_flush: bool,
    /// values and offsets capped at this many bytes (0 = no cap),
    pub max_len: u32,
    /// atomic write units capped at this many bytes (0 = no cap).
    pub unit_cap: u32,
}

impl StorageSimCfg {
    /// The parameters of `seed` under `f`.
    pub fn from_seed(seed: u64, f: &Filter) -> Self {
        let mut r = Rng::new(seed ^ 0xC0F1_6000);
        let profile = if f.smoke {
            [Profile::Mixed, Profile::Small, Profile::Churn, Profile::Full][r.weighted(&[40, 25, 20, 15])]
        } else {
            [Profile::Mixed, Profile::Small, Profile::Large, Profile::Full, Profile::Churn][r.weighted(&[38, 18, 14, 15, 15])]
        };
        let partitions = if f.smoke { 16 } else { [16u16, 32, 64, 128][r.weighted(&[70, 15, 10, 5])] };
        let devices = if r.chance(15) { 2 } else { 1 };
        let lba = f.lba.unwrap_or(if r.chance(50) { 512 } else { 4096 });
        let page_mode = f.mode.unwrap_or(if r.chance(45) { PageMode::Atomic } else { PageMode::TwoImage });
        let atomic_unit = match page_mode {
            PageMode::Atomic => Some([4096u32, 8192, 16384, 65536][r.weighted(&[60, 15, 10, 15])]),
            PageMode::TwoImage => match r.below(10) {
                0..=5 => None,
                6 | 7 => Some(lba),
                // the device could do atomic pages, the engine is told to use two images
                _ => Some(4096),
            },
        };
        let cache = f.cache.unwrap_or(if r.chance(60) { CacheMode::WriteBack } else { CacheMode::WriteThrough });
        let dsync = cache == CacheMode::WriteBack && r.chance(10);
        let durability = [Durability::Async, Durability::Written, Durability::Flushed][r.weighted(&[20, 40, 40])];
        let cache_bytes = [1usize << 20, 4 << 20, 16 << 20, 64 << 20][r.weighted(&[20, 35, 30, 15])];
        let base = u32::from(partitions) * 2;
        let ags_total = match profile {
            Profile::Full => base + r.range(1, 6) as u32,
            Profile::Large => base + r.range(8, 40) as u32,
            _ => base + r.range(4, 24) as u32,
        };
        let ags = ags_total.div_ceil(u32::from(devices)) + 1;
        let fill_pct = if profile == Profile::Full { r.range(40, 95) as u32 } else { 95 };
        let snapshot = r.chance(60);
        let keys = match profile {
            Profile::Small => r.range(50, 400),
            Profile::Mixed => r.range(30, 300),
            Profile::Large => r.range(6, 40),
            Profile::Full => r.range(20, 120),
            Profile::Churn => r.range(8, 60),
        } as u32;
        let keys = if f.smoke { keys.min(48) } else { keys };
        let groups = (keys / r.range(1, 6) as u32).max(2).min(keys);
        let dbs = if r.chance(60) { 1 } else { r.range(2, 3) as u8 };
        let mut steps = match profile {
            Profile::Large => r.range(150, 500),
            _ => r.range(300, 1500),
        } as u32;
        if f.smoke {
            steps = r.range(60, 180) as u32;
        }
        if f.scale > 0.0 {
            steps = ((steps as f32) * f.scale).max(10.0) as u32;
        }
        let rounds = r.range(1, 5) as u32;
        let mut c = StorageSimCfg {
            seed,
            partitions,
            devices,
            ags,
            lba,
            page_mode,
            atomic_unit,
            cache,
            dsync,
            durability,
            cache_bytes,
            fill_pct,
            snapshot,
            keys,
            groups,
            dbs,
            profile,
            steps,
            rounds,
            tick_permille: [40u32, 120, 250, 450][r.weighted(&[15, 40, 30, 15])],
            delay_max: [0u32, 4, 32, 200][r.weighted(&[20, 40, 30, 10])],
            late_pct: r.range(0, 60) as u32,
            skip_pct: [0u32, 10, 40][r.weighted(&[50, 35, 15])],
            per_poll: [0u32, 1, 2, 8][r.weighted(&[40, 20, 20, 20])],
            shuffle: r.chance(50),
            tick_ms: [1u32, 1, 2, 5][r.weighted(&[50, 20, 20, 10])],
            hang_ticks: 20_000,
            settle_ticks: 30_000,
            error_ppm: 0,
            no_flush: false,
            max_len: 0,
            unit_cap: 0,
        };
        if f.smoke {
            // small runs: tighter bounds keep the debug-build smoke fast
            c.hang_ticks = 4_000;
            c.settle_ticks = 6_000;
        }
        // drawn last: the parameters above stay those of earlier generator versions
        if !f.smoke && r.chance(10) {
            c.error_ppm = [200u32, 2000, 20_000][r.weighted(&[50, 35, 15])];
        }
        for (k, v) in &f.overrides {
            if let Err(e) = c.set(k, v) {
                eprintln!("--set {k}={v}: {e}");
            }
        }
        if c.unit_cap > 0 {
            c.atomic_unit = c.atomic_unit.map(|u| u.min(c.unit_cap.max(c.lba)));
        }
        c
    }

    /// Device size in pages: one 16 MiB unit of metadata, two snapshot regions, the AGs.
    fn device_pages(&self) -> u64 {
        let units = 1 + if self.snapshot { 2 } else { 0 } + u64::from(self.ags);
        units * 4096
    }

    fn snap_region_pages(&self) -> u64 {
        if self.snapshot { 4096 } else { 0 }
    }

    fn engine_cfg(&self) -> EngineCfg {
        let mut c = EngineCfg::defaults(Vec::new());
        c.durability = self.durability;
        c.cache_bytes = self.cache_bytes;
        c.max_storage_fill = self.fill_pct as f32 / 100.0;
        c.rates = BgRates::defaults(self.partitions);
        if self.dsync {
            c.flush_method = kv_storage::engine::FlushMethod::Dsync;
        }
        c
    }

    /// The matrix cell: page-update mode, device cache, lba.
    pub fn matrix(&self) -> String {
        format!(
            "{} {} lba{}",
            match self.page_mode {
                PageMode::Atomic => "atomic",
                PageMode::TwoImage => "two-image",
            },
            match self.cache {
                CacheMode::WriteThrough => "wt",
                CacheMode::WriteBack => "wb",
            },
            self.lba
        )
    }

    /// A one-line summary of the run's main parameters.
    pub fn cell(&self) -> String {
        format!(
            "{} {} lba{} {:?} p{}",
            match self.page_mode {
                PageMode::Atomic => "atomic",
                PageMode::TwoImage => "two-image",
            },
            match self.cache {
                CacheMode::WriteThrough => "wt",
                CacheMode::WriteBack => "wb",
            },
            self.lba,
            self.durability,
            self.partitions
        )
    }

    fn to_text(&self, out: &mut String) {
        let mut kv = |k: &str, v: String| {
            let _ = writeln!(out, "cfg {k} {v}");
        };
        kv("partitions", self.partitions.to_string());
        kv("devices", self.devices.to_string());
        kv("ags", self.ags.to_string());
        kv("lba", self.lba.to_string());
        kv("page_mode", match self.page_mode {
            PageMode::Atomic => "atomic".into(),
            PageMode::TwoImage => "two-image".into(),
        });
        kv("atomic_unit", self.atomic_unit.map_or("-".into(), |u| u.to_string()));
        kv("cache", match self.cache {
            CacheMode::WriteThrough => "wt".into(),
            CacheMode::WriteBack => "wb".into(),
        });
        kv("dsync", u8::from(self.dsync).to_string());
        kv("durability", match self.durability {
            Durability::Async => "async".into(),
            Durability::Written => "written".into(),
            Durability::Flushed => "flushed".into(),
        });
        kv("cache_bytes", self.cache_bytes.to_string());
        kv("fill_pct", self.fill_pct.to_string());
        kv("snapshot", u8::from(self.snapshot).to_string());
        kv("keys", self.keys.to_string());
        kv("groups", self.groups.to_string());
        kv("dbs", self.dbs.to_string());
        kv("profile", self.profile.name().into());
        kv("steps", self.steps.to_string());
        kv("rounds", self.rounds.to_string());
        kv("tick_permille", self.tick_permille.to_string());
        kv("delay_max", self.delay_max.to_string());
        kv("late_pct", self.late_pct.to_string());
        kv("skip_pct", self.skip_pct.to_string());
        kv("per_poll", self.per_poll.to_string());
        kv("shuffle", u8::from(self.shuffle).to_string());
        kv("tick_ms", self.tick_ms.to_string());
        kv("hang_ticks", self.hang_ticks.to_string());
        kv("settle_ticks", self.settle_ticks.to_string());
        kv("error_ppm", self.error_ppm.to_string());
        kv("no_flush", u8::from(self.no_flush).to_string());
        kv("max_len", self.max_len.to_string());
        kv("unit_cap", self.unit_cap.to_string());
    }

    fn set(&mut self, k: &str, v: &str) -> Result<(), String> {
        let n = || v.parse::<u64>().map_err(|_| format!("cfg {k}: bad number {v}"));
        match k {
            "partitions" => self.partitions = n()? as u16,
            "devices" => self.devices = n()? as u8,
            "ags" => self.ags = n()? as u32,
            "lba" => self.lba = n()? as u32,
            "page_mode" => self.page_mode = if v == "atomic" { PageMode::Atomic } else { PageMode::TwoImage },
            "atomic_unit" => self.atomic_unit = if v == "-" { None } else { Some(n()? as u32) },
            "cache" => self.cache = if v == "wt" { CacheMode::WriteThrough } else { CacheMode::WriteBack },
            "dsync" => self.dsync = n()? != 0,
            "durability" => {
                self.durability = match v {
                    "async" => Durability::Async,
                    "written" => Durability::Written,
                    _ => Durability::Flushed,
                }
            }
            "cache_bytes" => self.cache_bytes = n()? as usize,
            "fill_pct" => self.fill_pct = n()? as u32,
            "snapshot" => self.snapshot = n()? != 0,
            "keys" => self.keys = n()? as u32,
            "groups" => self.groups = n()? as u32,
            "dbs" => self.dbs = n()? as u8,
            "profile" => self.profile = Profile::parse(v).ok_or_else(|| format!("bad profile {v}"))?,
            "steps" => self.steps = n()? as u32,
            "rounds" => self.rounds = n()? as u32,
            "tick_permille" => self.tick_permille = n()? as u32,
            "delay_max" => self.delay_max = n()? as u32,
            "late_pct" => self.late_pct = n()? as u32,
            "skip_pct" => self.skip_pct = n()? as u32,
            "per_poll" => self.per_poll = n()? as u32,
            "shuffle" => self.shuffle = n()? != 0,
            "tick_ms" => self.tick_ms = n()? as u32,
            "hang_ticks" => self.hang_ticks = n()? as u32,
            "settle_ticks" => self.settle_ticks = n()? as u32,
            "error_ppm" => self.error_ppm = n()? as u32,
            "no_flush" => self.no_flush = n()? != 0,
            "max_len" => self.max_len = n()? as u32,
            "unit_cap" => self.unit_cap = n()? as u32,
            _ => return Err(format!("unknown cfg key {k}")),
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Steps

/// Expiry of a write.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Ttl {
    None,
    /// Relative, in ms.
    Rel(u64),
    /// Already past.
    Past,
}

impl Ttl {
    fn resolve(self, now: u64) -> Option<u64> {
        match self {
            Ttl::None => None,
            Ttl::Rel(ms) => Some(now + ms),
            Ttl::Past => Some(now.saturating_sub(1000)),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cond {
    Always,
    Nx,
    Xx,
}

/// A client command (keys are indices into the key table).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Cmd {
    Get { db: u8, k: u32 },
    /// EXISTS / STRLEN / PTTL.
    Meta { db: u8, k: u32 },
    /// SET with NX/XX, GET, KEEPTTL and EX/PX variants (SETNX, SETEX, GETSET).
    Set { db: u8, k: u32, len: u32, ttl: Ttl, cond: Cond, get: bool, keep: bool },
    Del { db: u8, ks: Vec<u32> },
    GetDel { db: u8, k: u32 },
    /// EXPIRE / PEXPIREAT; a past deadline deletes (`del_past`) or stores an expired record.
    Expire { db: u8, k: u32, ttl: Ttl, del_past: bool },
    Persist { db: u8, k: u32 },
    Rename { db: u8, src: u32, dst: u32, nx: bool },
    Copy { db: u8, src: u32, dst: u32, replace: bool },
    Mset { db: u8, kv: Vec<(u32, u32)> },
    Msetnx { db: u8, kv: Vec<(u32, u32)> },
    Append { db: u8, k: u32, len: u32 },
    SetRange { db: u8, k: u32, off: u64, len: u32 },
    /// FLUSHDB (`Some`) / FLUSHALL (`None`) on every partition.
    Flush { db: Option<u8> },
    /// A barrier on every partition (durable once everything before it is).
    Barrier,
    /// A full SCAN of one partition's db.
    Scan { db: u8, part: u16, count: u32 },
}

impl Cmd {
    pub fn name(&self) -> &'static str {
        match self {
            Cmd::Get { .. } => "GET",
            Cmd::Meta { .. } => "EXISTS",
            Cmd::Set { .. } => "SET",
            Cmd::Del { .. } => "DEL",
            Cmd::GetDel { .. } => "GETDEL",
            Cmd::Expire { .. } => "EXPIRE",
            Cmd::Persist { .. } => "PERSIST",
            Cmd::Rename { nx: false, .. } => "RENAME",
            Cmd::Rename { nx: true, .. } => "RENAMENX",
            Cmd::Copy { .. } => "COPY",
            Cmd::Mset { .. } => "MSET",
            Cmd::Msetnx { .. } => "MSETNX",
            Cmd::Append { .. } => "APPEND",
            Cmd::SetRange { .. } => "SETRANGE",
            Cmd::Flush { db: Some(_) } => "FLUSHDB",
            Cmd::Flush { db: None } => "FLUSHALL",
            Cmd::Barrier => "BARRIER",
            Cmd::Scan { .. } => "SCAN",
        }
    }

    fn db(&self) -> u8 {
        match self {
            Cmd::Get { db, .. }
            | Cmd::Meta { db, .. }
            | Cmd::Set { db, .. }
            | Cmd::Del { db, .. }
            | Cmd::GetDel { db, .. }
            | Cmd::Expire { db, .. }
            | Cmd::Persist { db, .. }
            | Cmd::Rename { db, .. }
            | Cmd::Copy { db, .. }
            | Cmd::Mset { db, .. }
            | Cmd::Msetnx { db, .. }
            | Cmd::Append { db, .. }
            | Cmd::SetRange { db, .. }
            | Cmd::Scan { db, .. } => *db,
            Cmd::Flush { db } => db.unwrap_or(0),
            Cmd::Barrier => 0,
        }
    }

    /// Keys and access modes of a keyed command (all of one hash-tag group).
    fn keys(&self) -> Vec<(u32, Access)> {
        match self {
            Cmd::Get { k, .. } => vec![(*k, Access::Read)],
            Cmd::Meta { k, .. } => vec![(*k, Access::Meta)],
            Cmd::Set { k, cond, get, keep, .. } => {
                let a = if *get || *keep || *cond != Cond::Always { Access::ReadWrite } else { Access::Write };
                vec![(*k, a)]
            }
            Cmd::Del { ks, .. } => ks.iter().map(|k| (*k, Access::Write)).collect(),
            Cmd::GetDel { k, .. } | Cmd::Expire { k, .. } | Cmd::Persist { k, .. } | Cmd::Append { k, .. } | Cmd::SetRange { k, .. } => {
                vec![(*k, Access::ReadWrite)]
            }
            Cmd::Rename { src, dst, .. } => vec![(*src, Access::ReadWrite), (*dst, Access::ReadWrite)],
            Cmd::Copy { src, dst, .. } => vec![(*src, Access::Read), (*dst, Access::ReadWrite)],
            Cmd::Mset { kv, .. } | Cmd::Msetnx { kv, .. } => kv.iter().map(|(k, _)| (*k, Access::Write)).collect(),
            Cmd::Flush { .. } | Cmd::Barrier | Cmd::Scan { .. } => Vec::new(),
        }
    }

    fn is_delete(&self) -> bool {
        matches!(self, Cmd::Del { .. } | Cmd::GetDel { .. } | Cmd::Flush { .. } | Cmd::Rename { .. })
            || matches!(self, Cmd::Expire { ttl: Ttl::Past, .. })
    }
}

/// One step of a run.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Step {
    /// Submit a command (pipelined: no ticks run).
    Cmd(Cmd),
    /// Drive this many ticks.
    Ticks(u32),
    /// Advance the clock (ms) without ticking.
    Clock(u32),
    /// I15 check point: no new input until every request is terminal, every write durable and
    /// every partition quiescent; then every key is read back and compared strictly.
    Quiesce,
    /// Crash after `after` more I/O completions: kill -9 or power cut, with `tear`.
    Crash { power: bool, tear: TearMode, after: u32 },
    /// Quiesce, clean shutdown (snapshot), restart, strict verification.
    Clean,
}

fn tear_name(t: TearMode) -> &'static str {
    match t {
        TearMode::None => "none",
        TearMode::Bytes => "bytes",
        TearMode::Sectors => "sectors",
        TearMode::Prefix => "prefix",
    }
}

fn ttl_text(t: Ttl) -> String {
    match t {
        Ttl::None => "-".into(),
        Ttl::Rel(ms) => format!("r{ms}"),
        Ttl::Past => "past".into(),
    }
}

fn kv_text(kv: &[(u32, u32)]) -> String {
    kv.iter().map(|(k, l)| format!("{k}:{l}")).collect::<Vec<_>>().join(",")
}

impl Step {
    pub fn to_text(&self) -> String {
        match self {
            Step::Ticks(n) => format!("ticks {n}"),
            Step::Clock(ms) => format!("clock {ms}"),
            Step::Quiesce => "quiesce".into(),
            Step::Clean => "clean".into(),
            Step::Crash { power, tear, after } => format!("crash {} {} {after}", if *power { "power" } else { "kill" }, tear_name(*tear)),
            Step::Cmd(c) => match c {
                Cmd::Get { db, k } => format!("cmd get {db} {k}"),
                Cmd::Meta { db, k } => format!("cmd meta {db} {k}"),
                Cmd::Set { db, k, len, ttl, cond, get, keep } => format!(
                    "cmd set {db} {k} {len} {} {} {} {}",
                    ttl_text(*ttl),
                    match cond {
                        Cond::Always => "-",
                        Cond::Nx => "nx",
                        Cond::Xx => "xx",
                    },
                    u8::from(*get),
                    u8::from(*keep)
                ),
                Cmd::Del { db, ks } => format!("cmd del {db} {}", ks.iter().map(u32::to_string).collect::<Vec<_>>().join(",")),
                Cmd::GetDel { db, k } => format!("cmd getdel {db} {k}"),
                Cmd::Expire { db, k, ttl, del_past } => format!("cmd expire {db} {k} {} {}", ttl_text(*ttl), u8::from(*del_past)),
                Cmd::Persist { db, k } => format!("cmd persist {db} {k}"),
                Cmd::Rename { db, src, dst, nx } => format!("cmd rename {db} {src} {dst} {}", u8::from(*nx)),
                Cmd::Copy { db, src, dst, replace } => format!("cmd copy {db} {src} {dst} {}", u8::from(*replace)),
                Cmd::Mset { db, kv } => format!("cmd mset {db} {}", kv_text(kv)),
                Cmd::Msetnx { db, kv } => format!("cmd msetnx {db} {}", kv_text(kv)),
                Cmd::Append { db, k, len } => format!("cmd append {db} {k} {len}"),
                Cmd::SetRange { db, k, off, len } => format!("cmd setrange {db} {k} {off} {len}"),
                Cmd::Flush { db } => format!("cmd flush {}", db.map_or("all".into(), |d| d.to_string())),
                Cmd::Barrier => "cmd barrier".into(),
                Cmd::Scan { db, part, count } => format!("cmd scan {db} {part} {count}"),
            },
        }
    }

    pub fn parse(line: &str) -> Result<Step, String> {
        let w: Vec<&str> = line.split_whitespace().collect();
        let bad = || format!("bad step: {line}");
        let num = |i: usize| -> Result<u64, String> { w.get(i).and_then(|s| s.parse().ok()).ok_or_else(bad) };
        let ttl = |i: usize| -> Result<Ttl, String> {
            match w.get(i).copied() {
                Some("-") => Ok(Ttl::None),
                Some("past") => Ok(Ttl::Past),
                Some(s) if s.starts_with('r') => s[1..].parse().map(Ttl::Rel).map_err(|_| bad()),
                _ => Err(bad()),
            }
        };
        let list = |i: usize| -> Result<Vec<u32>, String> {
            w.get(i).ok_or_else(bad)?.split(',').map(|s| s.parse().map_err(|_| bad())).collect()
        };
        let kvs = |i: usize| -> Result<Vec<(u32, u32)>, String> {
            w.get(i)
                .ok_or_else(bad)?
                .split(',')
                .map(|s| {
                    let (a, b) = s.split_once(':').ok_or_else(bad)?;
                    Ok((a.parse().map_err(|_| bad())?, b.parse().map_err(|_| bad())?))
                })
                .collect()
        };
        let tear = |s: &str| -> Result<TearMode, String> {
            Ok(match s {
                "none" => TearMode::None,
                "bytes" => TearMode::Bytes,
                "sectors" => TearMode::Sectors,
                "prefix" => TearMode::Prefix,
                _ => return Err(bad()),
            })
        };
        match w.first().copied() {
            Some("ticks") => Ok(Step::Ticks(num(1)? as u32)),
            Some("clock") => Ok(Step::Clock(num(1)? as u32)),
            Some("quiesce") => Ok(Step::Quiesce),
            Some("clean") => Ok(Step::Clean),
            Some("crash") => Ok(Step::Crash {
                power: w.get(1) == Some(&"power"),
                tear: tear(w.get(2).ok_or_else(bad)?)?,
                after: num(3)? as u32,
            }),
            Some("cmd") => {
                let db = || num(2).map(|d| d as u8);
                let c = match w.get(1).copied() {
                    Some("get") => Cmd::Get { db: db()?, k: num(3)? as u32 },
                    Some("meta") => Cmd::Meta { db: db()?, k: num(3)? as u32 },
                    Some("set") => Cmd::Set {
                        db: db()?,
                        k: num(3)? as u32,
                        len: num(4)? as u32,
                        ttl: ttl(5)?,
                        cond: match w.get(6).copied() {
                            Some("nx") => Cond::Nx,
                            Some("xx") => Cond::Xx,
                            _ => Cond::Always,
                        },
                        get: num(7)? != 0,
                        keep: num(8)? != 0,
                    },
                    Some("del") => Cmd::Del { db: db()?, ks: list(3)? },
                    Some("getdel") => Cmd::GetDel { db: db()?, k: num(3)? as u32 },
                    Some("expire") => Cmd::Expire { db: db()?, k: num(3)? as u32, ttl: ttl(4)?, del_past: num(5)? != 0 },
                    Some("persist") => Cmd::Persist { db: db()?, k: num(3)? as u32 },
                    Some("rename") => Cmd::Rename { db: db()?, src: num(3)? as u32, dst: num(4)? as u32, nx: num(5)? != 0 },
                    Some("copy") => Cmd::Copy { db: db()?, src: num(3)? as u32, dst: num(4)? as u32, replace: num(5)? != 0 },
                    Some("mset") => Cmd::Mset { db: db()?, kv: kvs(3)? },
                    Some("msetnx") => Cmd::Msetnx { db: db()?, kv: kvs(3)? },
                    Some("append") => Cmd::Append { db: db()?, k: num(3)? as u32, len: num(4)? as u32 },
                    Some("setrange") => Cmd::SetRange { db: db()?, k: num(3)? as u32, off: num(4)?, len: num(5)? as u32 },
                    Some("flush") => Cmd::Flush { db: if w.get(2) == Some(&"all") { None } else { Some(db()?) } },
                    Some("barrier") => Cmd::Barrier,
                    Some("scan") => Cmd::Scan { db: db()?, part: num(3)? as u16, count: num(4)? as u32 },
                    _ => return Err(bad()),
                };
                Ok(Step::Cmd(c))
            }
            _ => Err(bad()),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Workload generation

/// A value length for `p`.
fn pick_len(p: Profile, r: &mut Rng) -> u32 {
    // tiny, small, border, page, medium, large, huge, chunked
    let w: [u32; 8] = match p {
        Profile::Small => [40, 55, 5, 0, 0, 0, 0, 0],
        Profile::Mixed => [200, 450, 100, 80, 100, 50, 15, 5],
        Profile::Large => [0, 200, 50, 100, 250, 250, 100, 50],
        Profile::Full => [0, 200, 0, 0, 300, 400, 100, 0],
        Profile::Churn => [20, 40, 20, 10, 10, 0, 0, 0],
    };
    match r.weighted(&w) {
        0 => r.range(0, 32) as u32,
        1 => r.range(33, 1500) as u32,
        2 => r.range(1900, 2100) as u32,
        3 => r.range(2100, 4096) as u32,
        4 => r.range(4097, 64 << 10) as u32,
        5 => r.range(64 << 10, 1 << 20) as u32,
        6 => r.range(1 << 20, (8 << 20) - 200) as u32,
        _ => r.range((8 << 20) - 100, 12 << 20) as u32,
    }
}

fn pick_ttl(r: &mut Rng) -> Ttl {
    match r.below(100) {
        0..=59 => Ttl::None,
        60..=74 => Ttl::Rel(r.range(1, 200)),
        75..=86 => Ttl::Rel(r.range(200, 5000)),
        87..=95 => Ttl::Rel(r.range(3_600_000, 86_400_000)),
        _ => Ttl::Past,
    }
}

struct Gen<'a> {
    cfg: &'a StorageSimCfg,
    r: Rng,
}

impl Gen<'_> {
    fn key(&mut self) -> u32 {
        let n = self.cfg.keys.max(1);
        if self.r.chance(40) { self.r.below(u64::from((n / 8).max(1))) as u32 } else { self.r.below(u64::from(n)) as u32 }
    }

    /// A key of the same hash-tag group as `k`.
    fn sibling(&mut self, k: u32) -> u32 {
        let g = self.cfg.groups.max(1);
        let base = k % g;
        let members = (self.cfg.keys.saturating_sub(base)).div_ceil(g).max(1);
        base + g * self.r.below(u64::from(members)) as u32
    }

    fn db(&mut self) -> u8 {
        if self.cfg.dbs <= 1 || self.r.chance(60) { 0 } else { self.r.below(u64::from(self.cfg.dbs)) as u8 }
    }

    fn cmd(&mut self) -> Cmd {
        let p = self.cfg.profile;
        // get, meta, set, del, getdel, expire, persist, rename, copy, mset, msetnx, append,
        // setrange, flushdb, flushall, barrier, scan
        let w: [u32; 17] = match p {
            Profile::Full => [80, 20, 400, 200, 20, 30, 10, 20, 10, 40, 20, 60, 40, 3, 1, 5, 10],
            Profile::Churn => [100, 30, 300, 100, 30, 60, 20, 50, 20, 60, 30, 80, 60, 15, 5, 10, 20],
            _ => [120, 40, 300, 70, 20, 60, 20, 40, 20, 60, 30, 60, 50, 4, 1, 5, 20],
        };
        let mut w = w;
        if self.cfg.no_flush {
            w[13] = 0;
            w[14] = 0;
        }
        let db = self.db();
        let cmd = match self.r.weighted(&w) {
            0 => Cmd::Get { db, k: self.key() },
            1 => Cmd::Meta { db, k: self.key() },
            2 => {
                let cond = match self.r.below(10) {
                    0 => Cond::Nx,
                    1 => Cond::Xx,
                    _ => Cond::Always,
                };
                Cmd::Set {
                    db,
                    k: self.key(),
                    len: pick_len(p, &mut self.r),
                    ttl: pick_ttl(&mut self.r),
                    cond,
                    get: self.r.chance(10),
                    keep: self.r.chance(5),
                }
            }
            3 => {
                let k = self.key();
                let n = self.r.range(1, 3);
                let mut ks = vec![k];
                for _ in 1..n {
                    ks.push(self.sibling(k));
                }
                Cmd::Del { db, ks }
            }
            4 => Cmd::GetDel { db, k: self.key() },
            5 => Cmd::Expire { db, k: self.key(), ttl: pick_ttl(&mut self.r), del_past: self.r.chance(50) },
            6 => Cmd::Persist { db, k: self.key() },
            7 => {
                let src = self.key();
                Cmd::Rename { db, src, dst: self.sibling(src), nx: self.r.chance(30) }
            }
            8 => {
                let src = self.key();
                Cmd::Copy { db, src, dst: self.sibling(src), replace: self.r.chance(50) }
            }
            9 | 10 => {
                let k = self.key();
                let n = self.r.range(2, 4);
                let mut kv = vec![(k, pick_len(p, &mut self.r))];
                for _ in 1..n {
                    let s = self.sibling(k);
                    kv.push((s, pick_len(p, &mut self.r)));
                }
                // multi-key writes of several MiB per key are rare
                if kv.iter().map(|(_, l)| u64::from(*l)).sum::<u64>() > (16 << 20) {
                    kv.truncate(1);
                }
                if self.r.below(2) == 0 { Cmd::Mset { db, kv } } else { Cmd::Msetnx { db, kv } }
            }
            11 => {
                let len = match self.r.below(20) {
                    0 => self.r.range(1900, 2200),
                    1 if p != Profile::Small => self.r.range(4096, 100_000),
                    _ => self.r.range(1, 300),
                } as u32;
                Cmd::Append { db, k: self.key(), len }
            }
            12 => {
                let off = match self.r.below(20) {
                    0 if matches!(p, Profile::Large | Profile::Mixed) => self.r.range((8 << 20) - 5000, (8 << 20) + 5000),
                    1 | 2 if p != Profile::Small => self.r.range(2000, 70_000),
                    _ => self.r.range(0, 1200),
                };
                Cmd::SetRange { db, k: self.key(), off, len: self.r.range(1, 600) as u32 }
            }
            13 => Cmd::Flush { db: Some(db) },
            14 => Cmd::Flush { db: None },
            15 => Cmd::Barrier,
            _ => Cmd::Scan { db, part: self.r.below(u64::from(self.cfg.partitions)) as u16, count: self.r.range(1, 50) as u32 },
        };
        self.capped(cmd)
    }

    /// `cmd` with lengths and offsets within `max_len`.
    fn capped(&self, mut cmd: Cmd) -> Cmd {
        let m = self.cfg.max_len;
        if m == 0 {
            return cmd;
        }
        match &mut cmd {
            Cmd::Set { len, .. } | Cmd::Append { len, .. } => *len = (*len).min(m),
            Cmd::SetRange { off, len, .. } => {
                *len = (*len).min(m);
                *off = (*off).min(u64::from(m.saturating_sub(*len)));
            }
            Cmd::Mset { kv, .. } | Cmd::Msetnx { kv, .. } => {
                for (_, l) in kv.iter_mut() {
                    *l = (*l).min(m);
                }
            }
            _ => {}
        }
        cmd
    }

    fn crash(&mut self) -> Step {
        let power = self.r.chance(55);
        // a killed process leaves its submitted writes whole, cancelled, or (split requests)
        // partly done; byte-level tearing is a power-loss outcome only
        let tear = if power {
            [TearMode::None, TearMode::Bytes, TearMode::Sectors, TearMode::Prefix][self.r.below(4) as usize]
        } else {
            [TearMode::None, TearMode::None, TearMode::Sectors, TearMode::Prefix][self.r.below(4) as usize]
        };
        let after = match self.r.below(10) {
            0..=2 => 0,
            3..=7 => self.r.range(1, 20),
            _ => self.r.range(20, 600),
        } as u32;
        Step::Crash { power, tear, after }
    }
}

/// The step list of `cfg` (deterministic in `cfg.seed`).
pub fn generate_steps(cfg: &StorageSimCfg) -> Vec<Step> {
    let mut g = Gen { cfg, r: Rng::new(cfg.seed ^ 0x57E9_5EED) };
    let rounds = cfg.rounds.max(1);
    let per_round = (cfg.steps / rounds).max(1);
    let mut steps = Vec::with_capacity(cfg.steps as usize + 8);
    for round in 0..rounds {
        for _ in 0..per_round {
            let x = g.r.below(1000) as u32;
            let t = cfg.tick_permille;
            if x < t {
                let n = if g.r.chance(80) { g.r.range(1, 8) } else { g.r.range(8, 60) };
                steps.push(Step::Ticks(n as u32));
            } else if x < t + 8 {
                let ms = match g.r.below(20) {
                    0 => g.r.range(10_000, 100_000),
                    1..=5 => g.r.range(100, 2000),
                    _ => g.r.range(1, 50),
                };
                steps.push(Step::Clock(ms as u32));
            } else if x < t + 14 {
                steps.push(Step::Quiesce);
            } else {
                steps.push(Step::Cmd(g.cmd()));
            }
        }
        if round + 1 < rounds {
            if g.r.chance(15) {
                steps.push(Step::Clean);
            } else {
                let c = g.crash();
                steps.push(c);
            }
        }
    }
    steps
}

// ---------------------------------------------------------------------------------------------
// The device: SimDevice plus late and out-of-order completions

/// `EIO` (injected device errors).
const EIO: i32 = 5;

/// Shared I/O control of every device of a run (single-threaded).
struct IoCtl {
    rng: Rng,
    polls: u64,
    delivered: u64,
    /// No completion is delivered (except to synchronous waits) once `delivered` reaches it.
    freeze_at: Option<u64>,
    delay_max: u32,
    late_pct: u32,
    skip_pct: u32,
    error_ppm: u32,
    errors_on: bool,
    errors: u64,
}

/// A `SimDevice` whose completions reach the engine late (held back for up to `delay_max`
/// polls) and in a random order, and which can stop delivering at an exact completion count
/// (the crash point).
pub struct SimDev {
    inner: SimDevice,
    held: Vec<(u64, IoDone)>,
    ctl: Rc<RefCell<IoCtl>>,
}

impl SimDev {
    /// Requests submitted and not delivered yet.
    fn outstanding(&self) -> usize {
        self.inner.pending() + self.held.len()
    }

    /// The device's queues (hang reports).
    fn describe(&self) -> String {
        let c = self.ctl.borrow();
        let due = |f: fn(u64, u64) -> u64| self.held.iter().map(|h| h.0).reduce(f);
        format!(
            "in device {}, completed but held {} (due {:?}..{:?}, now {}), volatile sectors {}, delivered {}, freeze {:?}",
            self.inner.pending(),
            self.held.len(),
            due(u64::min),
            due(u64::max),
            c.polls,
            self.inner.volatile_sectors(),
            c.delivered,
            c.freeze_at
        )
    }

    fn crash(&mut self, power: bool, tear: TearMode) {
        self.held.clear();
        if power {
            self.inner.power_cut(tear);
        } else {
            self.inner.crash(tear);
        }
    }
}

impl BlockDevice for SimDev {
    fn info(&self) -> DevInfo {
        self.inner.info()
    }

    fn submit(&mut self, r: IoReq) {
        let mut guard = self.ctl.borrow_mut();
        let c = &mut *guard;
        if c.errors_on && c.error_ppm > 0 && c.rng.below(1_000_000) < u64::from(c.error_ppm) {
            // the request fails without changing anything
            c.errors += 1;
            let due = c.polls + 1;
            let tag = r.tag;
            self.held.push((due, IoDone { tag, res: Err(-EIO), buf: r.into_buf() }));
            return;
        }
        self.inner.submit(r);
    }

    fn poll(&mut self, out: &mut Vec<IoDone>, wait: bool) {
        let mut guard = self.ctl.borrow_mut();
        let c = &mut *guard;
        c.polls += 1;
        let now = c.polls;
        let skip = !wait && c.skip_pct > 0 && c.rng.chance(c.skip_pct);
        if !skip {
            let mut got = Vec::new();
            self.inner.poll(&mut got, wait);
            for d in got {
                let late = !wait && c.delay_max > 0 && c.rng.chance(c.late_pct);
                let due = if late { now + 1 + c.rng.below(u64::from(c.delay_max)) } else { now };
                self.held.push((due, d));
            }
        }
        if !wait && c.freeze_at.is_some_and(|f| c.delivered >= f) {
            return;
        }
        if self.held.is_empty() {
            return;
        }
        let held = core::mem::take(&mut self.held);
        let mut due = Vec::new();
        for (t, d) in held {
            if wait || t <= now {
                due.push((t, d));
            } else {
                self.held.push((t, d));
            }
        }
        c.rng.shuffle(&mut due);
        if !wait && let Some(f) = c.freeze_at {
            let budget = f.saturating_sub(c.delivered) as usize;
            if due.len() > budget {
                self.held.extend(due.drain(budget..));
            }
        }
        c.delivered += due.len() as u64;
        out.extend(due.into_iter().map(|(_, d)| d));
    }
}

// ---------------------------------------------------------------------------------------------
// Failures

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub enum FailKind {
    /// I15: an accepted request did not reach a terminal state within the bound.
    Hang,
    /// An executed write never became durable (no request waits for it, but nothing will).
    Stall,
    /// Every request is terminal and durable but a partition never became quiescent.
    NoQuiesce,
    /// A command observed a state the model does not allow (live, not after a crash).
    Result,
    I1,
    I2,
    I4,
    I10,
    /// Bounded copies on media (and a durable copy for every live key) after the final quiesce.
    I11,
    /// The engine refused to open or recover the devices.
    Recover,
    /// The engine broke its API contract (write without ticket, unknown ready request, an
    /// I/O error or failed ticket without injected errors, a corrupt page).
    Engine,
    /// The run panicked.
    Panic,
    /// The run exceeded its wall-clock budget (a loop inside the engine).
    Wall,
}

impl FailKind {
    pub fn name(self) -> &'static str {
        match self {
            FailKind::Hang => "HANG",
            FailKind::Stall => "STALL",
            FailKind::NoQuiesce => "NOQUIESCE",
            FailKind::Result => "RESULT",
            FailKind::I1 => "I1",
            FailKind::I2 => "I2",
            FailKind::I4 => "I4",
            FailKind::I10 => "I10",
            FailKind::I11 => "I11",
            FailKind::Recover => "RECOVER",
            FailKind::Engine => "ENGINE",
            FailKind::Panic => "PANIC",
            FailKind::Wall => "WALL",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        [
            FailKind::Hang,
            FailKind::Stall,
            FailKind::NoQuiesce,
            FailKind::Result,
            FailKind::I1,
            FailKind::I2,
            FailKind::I4,
            FailKind::I10,
            FailKind::I11,
            FailKind::Recover,
            FailKind::Engine,
            FailKind::Panic,
            FailKind::Wall,
        ]
        .into_iter()
        .find(|k| k.name() == s)
    }
}

/// A failed run.
#[derive(Clone, Debug)]
pub struct Failure {
    pub kind: FailKind,
    /// Index of the step at (or after) which it was detected (`steps.len()` = the final check).
    pub step: usize,
    /// A short class of the failure (request kind and wait phase for hangs).
    pub sig: String,
    pub detail: String,
}

impl Failure {
    fn new(kind: FailKind, sig: impl Into<String>, detail: impl Into<String>) -> Self {
        Failure { kind, step: 0, sig: sig.into(), detail: detail.into() }
    }

    /// One line: `KIND step sig :: detail` (newlines of the detail become ` | `).
    pub fn line(&self) -> String {
        format!("{} step={} [{}] :: {}", self.kind.name(), self.step, self.sig, self.detail.replace('\n', " | "))
    }
}

/// Counters of a passed run.
#[derive(Clone, Debug, Default)]
pub struct RunStats {
    pub ticks: u64,
    pub commands: u64,
    pub requeues: u64,
    pub max_latency: u64,
    pub crashes: u64,
    pub cleans: u64,
    pub snapshot_restarts: u64,
    pub out_of_space: u64,
    pub other_errors: u64,
    pub quiesces: u64,
    pub keys_checked: u64,
}

// ---------------------------------------------------------------------------------------------
// Command semantics, written once for the store and for the model

type Val = Option<(Vec<u8>, Option<u64>)>;

/// The storage operations a command uses.
trait Kv {
    fn now(&self) -> u64;
    fn get(&mut self, db: u8, k: u32) -> Result<Val, StoreError>;
    fn meta(&mut self, db: u8, k: u32) -> Result<Option<(u64, Option<u64>)>, StoreError>;
    fn put(&mut self, db: u8, k: u32, v: &[u8], at: Option<u64>) -> Result<(), StoreError>;
    fn append(&mut self, db: u8, k: u32, d: &[u8]) -> Result<u64, StoreError>;
    fn set_range(&mut self, db: u8, k: u32, off: u64, d: &[u8]) -> Result<u64, StoreError>;
    fn set_expire(&mut self, db: u8, k: u32, at: Option<u64>) -> Result<bool, StoreError>;
    fn delete(&mut self, db: u8, k: u32) -> Result<bool, StoreError>;
}

const NONE: u64 = u64::MAX;

fn obs_val(o: &mut Vec<u64>, v: &Val) {
    match v {
        None => o.push(0),
        Some((b, at)) => {
            o.push(1);
            o.push(b.len() as u64);
            o.push(crate::model::fingerprint(b));
            o.push(at.unwrap_or(NONE));
        }
    }
}

/// Runs `cmd` (step `uid`) against `kv`; returns what a client would observe.
fn apply_cmd(kv: &mut dyn Kv, cmd: &Cmd, uid: u64) -> Result<Vec<u64>, StoreError> {
    let mut o = Vec::new();
    let now = kv.now();
    match cmd {
        Cmd::Get { db, k } => {
            let v = kv.get(*db, *k)?;
            obs_val(&mut o, &v);
        }
        Cmd::Meta { db, k } => match kv.meta(*db, *k)? {
            None => o.push(0),
            Some((len, at)) => o.extend([1, len, at.unwrap_or(NONE)]),
        },
        Cmd::Set { db, k, len, ttl, cond, get, keep } => {
            let old = if *get { kv.get(*db, *k)? } else { kv.meta(*db, *k)?.map(|(_, at)| (Vec::new(), at)) };
            if *get {
                obs_val(&mut o, &old);
            }
            let go = match cond {
                Cond::Always => true,
                Cond::Nx => old.is_none(),
                Cond::Xx => old.is_some(),
            };
            o.push(u64::from(go));
            if go {
                let at = if *keep { old.as_ref().and_then(|(_, a)| *a) } else { ttl.resolve(now) };
                kv.put(*db, *k, &value(uid, 0, *len as usize), at)?;
            }
        }
        Cmd::Del { db, ks } => {
            let mut n = 0;
            for k in ks {
                n += u64::from(kv.delete(*db, *k)?);
            }
            o.push(n);
        }
        Cmd::GetDel { db, k } => {
            let v = kv.get(*db, *k)?;
            if v.is_some() {
                kv.delete(*db, *k)?;
            }
            obs_val(&mut o, &v);
        }
        Cmd::Expire { db, k, ttl, del_past } => {
            let at = ttl.resolve(now);
            let r = match at {
                Some(t) if t <= now && *del_past => kv.delete(*db, *k)?,
                _ => kv.set_expire(*db, *k, at)?,
            };
            o.push(u64::from(r));
        }
        Cmd::Persist { db, k } => match kv.meta(*db, *k)? {
            Some((_, Some(_))) => o.push(u64::from(kv.set_expire(*db, *k, None)?)),
            _ => o.push(0),
        },
        Cmd::Rename { db, src, dst, nx } => match kv.get(*db, *src)? {
            None => o.push(NONE),
            Some(_) if src == dst => o.push(u64::from(!*nx)),
            Some((v, at)) => {
                if *nx && kv.meta(*db, *dst)?.is_some() {
                    o.push(0);
                } else {
                    kv.delete(*db, *src)?;
                    kv.put(*db, *dst, &v, at)?;
                    o.push(1);
                }
            }
        },
        Cmd::Copy { db, src, dst, replace } => match kv.get(*db, *src)? {
            None => o.push(0),
            Some(_) if src == dst => o.push(0),
            Some((v, at)) => {
                if !*replace && kv.meta(*db, *dst)?.is_some() {
                    o.push(0);
                } else {
                    kv.put(*db, *dst, &v, at)?;
                    o.push(1);
                }
            }
        },
        Cmd::Mset { db, kv: pairs } => {
            for (j, (k, len)) in pairs.iter().enumerate() {
                kv.put(*db, *k, &value(uid, j as u32, *len as usize), None)?;
            }
            o.push(1);
        }
        Cmd::Msetnx { db, kv: pairs } => {
            let mut any = false;
            for (k, _) in pairs {
                any |= kv.meta(*db, *k)?.is_some();
            }
            if !any {
                for (j, (k, len)) in pairs.iter().enumerate() {
                    kv.put(*db, *k, &value(uid, j as u32, *len as usize), None)?;
                }
            }
            o.push(u64::from(!any));
        }
        Cmd::Append { db, k, len } => o.push(kv.append(*db, *k, &value(uid, 0, *len as usize))?),
        Cmd::SetRange { db, k, off, len } => o.push(kv.set_range(*db, *k, *off, &value(uid, 0, *len as usize))?),
        Cmd::Flush { .. } | Cmd::Barrier | Cmd::Scan { .. } => {}
    }
    Ok(o)
}

/// [`Kv`] over a partition's keyspace.
struct StoreKv<'a, 'b> {
    ks: &'a mut Keyspace<'b>,
    keytab: &'a [Vec<u8>],
}

impl Kv for StoreKv<'_, '_> {
    fn now(&self) -> u64 {
        self.ks.now_ms()
    }
    fn get(&mut self, db: u8, k: u32) -> Result<Val, StoreError> {
        self.ks.select(db);
        Ok(self.ks.get(&self.keytab[k as usize])?.map(|(m, v)| (v.to_vec(), m.expire_at_ms)))
    }
    fn meta(&mut self, db: u8, k: u32) -> Result<Option<(u64, Option<u64>)>, StoreError> {
        self.ks.select(db);
        Ok(self.ks.meta(&self.keytab[k as usize])?.map(|m| (m.len, m.expire_at_ms)))
    }
    fn put(&mut self, db: u8, k: u32, v: &[u8], at: Option<u64>) -> Result<(), StoreError> {
        self.ks.select(db);
        self.ks.put(&self.keytab[k as usize], v, VType::String, at)
    }
    fn append(&mut self, db: u8, k: u32, d: &[u8]) -> Result<u64, StoreError> {
        self.ks.select(db);
        self.ks.append(&self.keytab[k as usize], d)
    }
    fn set_range(&mut self, db: u8, k: u32, off: u64, d: &[u8]) -> Result<u64, StoreError> {
        self.ks.select(db);
        self.ks.set_range(&self.keytab[k as usize], off, d)
    }
    fn set_expire(&mut self, db: u8, k: u32, at: Option<u64>) -> Result<bool, StoreError> {
        self.ks.select(db);
        self.ks.set_expire(&self.keytab[k as usize], at)
    }
    fn delete(&mut self, db: u8, k: u32) -> Result<bool, StoreError> {
        self.ks.select(db);
        self.ks.delete(&self.keytab[k as usize])
    }
}

/// [`Kv`] over the model: reads see the command's own staged writes first.
struct ModelKv<'a> {
    model: &'a RefModel,
    now: u64,
    staged: Vec<(KeyRef, Val)>,
}

impl ModelKv<'_> {
    fn cur(&self, k: KeyRef) -> Val {
        if let Some((_, v)) = self.staged.iter().rev().find(|(x, _)| *x == k) {
            return match v {
                Some((b, at)) if at.is_none_or(|t| t > self.now) => Some((b.clone(), *at)),
                _ => None,
            };
        }
        self.model.cur_live(k, self.now).map(|(b, at)| (b.to_vec(), at))
    }

    fn stage(&mut self, k: KeyRef, v: Val) {
        self.staged.retain(|(x, _)| *x != k);
        self.staged.push((k, v));
    }
}

impl Kv for ModelKv<'_> {
    fn now(&self) -> u64 {
        self.now
    }
    fn get(&mut self, db: u8, k: u32) -> Result<Val, StoreError> {
        Ok(self.cur((db, k)))
    }
    fn meta(&mut self, db: u8, k: u32) -> Result<Option<(u64, Option<u64>)>, StoreError> {
        Ok(self.cur((db, k)).map(|(b, at)| (b.len() as u64, at)))
    }
    fn put(&mut self, db: u8, k: u32, v: &[u8], at: Option<u64>) -> Result<(), StoreError> {
        self.stage((db, k), Some((v.to_vec(), at)));
        Ok(())
    }
    fn append(&mut self, db: u8, k: u32, d: &[u8]) -> Result<u64, StoreError> {
        let (mut v, at) = self.cur((db, k)).unwrap_or((Vec::new(), None));
        v.extend_from_slice(d);
        let n = v.len() as u64;
        self.stage((db, k), Some((v, at)));
        Ok(n)
    }
    fn set_range(&mut self, db: u8, k: u32, off: u64, d: &[u8]) -> Result<u64, StoreError> {
        let cur = self.cur((db, k));
        let (mut v, at) = match cur {
            None if d.is_empty() => return Ok(0),
            None => (Vec::new(), None),
            Some(x) => x,
        };
        if d.is_empty() {
            return Ok(v.len() as u64);
        }
        let end = off as usize + d.len();
        if v.len() < end {
            v.resize(end, 0);
        }
        v[off as usize..end].copy_from_slice(d);
        let n = v.len() as u64;
        self.stage((db, k), Some((v, at)));
        Ok(n)
    }
    fn set_expire(&mut self, db: u8, k: u32, at: Option<u64>) -> Result<bool, StoreError> {
        match self.cur((db, k)) {
            None => Ok(false),
            Some((v, _)) => {
                self.stage((db, k), Some((v, at)));
                Ok(true)
            }
        }
    }
    fn delete(&mut self, db: u8, k: u32) -> Result<bool, StoreError> {
        match self.cur((db, k)) {
            None => Ok(false),
            Some(_) => {
                self.stage((db, k), None);
                Ok(true)
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The driver

/// When a part's reply may be released.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Need {
    Exec,
    Written,
    Durable,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    /// Submitted; not returned by `take_ready` yet (or re-queued after `Requeued`).
    Queued,
    /// Executed; waiting for its ticket.
    Ticket(TicketId),
    Done,
}

struct PartRun {
    p: u16,
    id: Option<ReqId>,
    phase: Phase,
    need: Need,
    requeues: u32,
    /// SCAN: next cursor.
    cursor: u64,
}

/// What a command's results go to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Sink {
    /// A workload command: compared with the model, applied to it.
    Model,
    /// A verification read: the value is collected.
    Probe,
    /// A verification SCAN: the keys are collected.
    ScanAll,
}

struct CmdRun {
    cmd: Cmd,
    sink: Sink,
    parts: Vec<PartRun>,
    submitted: u64,
    step: usize,
}

/// A ticket not final yet.
struct OpenTicket {
    p: u16,
    t: TicketId,
    op: u64,
    since: u64,
    cmd: &'static str,
}

/// Probe uids start here (step uids are step indices).
const PROBE_UID: u64 = 1 << 40;

struct Sim<'c> {
    cfg: &'c StorageSimCfg,
    ecfg: EngineCfg,
    eng: Option<LocalEngine<SimDev>>,
    ctl: Rc<RefCell<IoCtl>>,
    keytab: Vec<Vec<u8>>,
    keypart: Vec<u16>,
    bykey: BTreeMap<Vec<u8>, u32>,
    model: RefModel,
    now_ms: u64,
    ticks: u64,
    out: BTreeMap<u64, CmdRun>,
    by_req: BTreeMap<(u16, u32), (u64, usize)>,
    tickets: Vec<OpenTicket>,
    probes: BTreeMap<u64, Val>,
    /// Probes that ended with an error reply (runs with injected errors): what they read is
    /// unknown, not "absent".
    probe_failed: BTreeSet<u64>,
    scanned: BTreeSet<u32>,
    next_probe: u64,
    rng: Rng,
    step: usize,
    stats: RunStats,
    /// The clock stands still (verification: GET and SCAN must see one instant).
    frozen_clock: bool,
    /// `KV_SIM_TRACE`: one stderr line per executed command (lengths and fingerprints only).
    trace: bool,
    /// Completions delivered when the current drain started.
    drain_from: u64,
}

fn key_bytes(i: u32, groups: u32) -> Vec<u8> {
    let g = i % groups.max(1);
    let mut k = format!("{{t{g}}}k{i}").into_bytes();
    if i % 7 == 3 {
        k.extend(std::iter::repeat_n(b'x', (i as usize * 13) % 180));
    }
    k
}

fn fmt_store_err(e: StoreError) -> String {
    format!("{e}")
}

impl<'c> Sim<'c> {
    fn new(cfg: &'c StorageSimCfg) -> Result<Self, Failure> {
        let ctl = Rc::new(RefCell::new(IoCtl {
            rng: Rng::new(cfg.seed ^ 0x10C7_1000),
            polls: 0,
            delivered: 0,
            freeze_at: None,
            delay_max: cfg.delay_max,
            late_pct: cfg.late_pct,
            skip_pct: cfg.skip_pct,
            error_ppm: cfg.error_ppm,
            errors_on: false,
            errors: 0,
        }));
        let pages = cfg.device_pages();
        let mut devs: Vec<SimDev> = (0..cfg.devices)
            .map(|i| {
                let mut d = SimDevice::new(pages, cfg.lba, cfg.atomic_unit, cfg.seed.wrapping_add(u64::from(i) * 7919));
                if cfg.page_mode == PageMode::TwoImage {
                    d.set_page_update(PageUpdateMode::TwoImage);
                } else {
                    d.set_page_update(PageUpdateMode::Atomic);
                }
                if cfg.per_poll > 0 {
                    d.set_max_completions(cfg.per_poll as usize);
                }
                let d = d.with_write_through(cfg.cache == CacheMode::WriteThrough);
                SimDev { inner: d, held: Vec::new(), ctl: ctl.clone() }
            })
            .collect();
        {
            let mut refs: Vec<&mut dyn BlockDevice> = devs.iter_mut().map(|d| d as &mut dyn BlockDevice).collect();
            format_devices(&mut refs, cfg.partitions, false, cfg.snap_region_pages(), cfg.seed)
                .map_err(|e| Failure::new(FailKind::Recover, "format", format!("format_devices: {e}")))?;
        }
        let keytab: Vec<Vec<u8>> = (0..cfg.keys).map(|i| key_bytes(i, cfg.groups)).collect();
        let bykey = keytab.iter().enumerate().map(|(i, k)| (k.clone(), i as u32)).collect();
        let keypart = keytab.iter().map(|k| kv_common::key_slot(k) % cfg.partitions).collect();
        let mut sim = Sim {
            cfg,
            ecfg: cfg.engine_cfg(),
            eng: None,
            ctl,
            keytab,
            keypart,
            bykey,
            model: RefModel::new(),
            now_ms: T0,
            ticks: 0,
            out: BTreeMap::new(),
            by_req: BTreeMap::new(),
            tickets: Vec::new(),
            probes: BTreeMap::new(),
            probe_failed: BTreeSet::new(),
            scanned: BTreeSet::new(),
            next_probe: PROBE_UID,
            rng: Rng::new(cfg.seed ^ 0xD71F_E400),
            step: 0,
            stats: RunStats::default(),
            frozen_clock: false,
            trace: std::env::var_os("KV_SIM_TRACE").is_some(),
            drain_from: 0,
        };
        sim.open(devs)?;
        Ok(sim)
    }

    fn eng(&mut self) -> &mut LocalEngine<SimDev> {
        self.eng.as_mut().expect("engine open")
    }

    fn clock(&self) -> Now {
        let ns = (self.now_ms - T0 + 1) * 1_000_000;
        Now { mono_ns: ns, boot_ns: ns, unix_ms: self.now_ms }
    }

    fn advance(&mut self, ms: u64) {
        self.now_ms += ms;
        let now = self.clock();
        if let Some(e) = self.eng.as_mut() {
            e.set_now(now);
        }
    }

    /// Opens and recovers the engine over `devs` (no injected errors meanwhile).
    fn open(&mut self, devs: Vec<SimDev>) -> Result<RecoveryMode, Failure> {
        self.set_errors(false);
        let mut eng = LocalEngine::open(devs, &self.ecfg).map_err(|e| Failure::new(FailKind::Recover, "open", format!("LocalEngine::open: {e}")))?;
        eng.set_now(self.clock());
        let mode = eng.recover().map_err(|e| Failure::new(FailKind::Recover, "recover", format!("recover: {e}")))?;
        for p in 0..eng.partitions() {
            let part = eng.partition(p);
            if part.state() != kv_storage::partition::PartState::Ready {
                return Err(Failure::new(
                    FailKind::Hang,
                    "recovery",
                    format!("partition {p} is {:?} after recover() returned: {}", part.state(), part.diag()),
                ));
            }
        }
        self.eng = Some(eng);
        Ok(mode)
    }

    // -----------------------------------------------------------------------------------------
    // submission and execution

    fn submit(&mut self, uid: u64, cmd: Cmd, sink: Sink) {
        let parts_n = self.cfg.partitions;
        let mut reqs: Vec<(u16, Request, u64)> = Vec::new();
        match &cmd {
            Cmd::Flush { db } => {
                for p in 0..parts_n {
                    reqs.push((p, Request { db: db.unwrap_or(0), keys: Default::default(), kind: ReqKind::FlushDb { db: *db }, origin: Origin::Client }, 0));
                }
            }
            Cmd::Barrier => {
                for p in 0..parts_n {
                    reqs.push((p, Request { db: 0, keys: Default::default(), kind: ReqKind::Barrier, origin: Origin::Client }, 0));
                }
            }
            Cmd::Scan { db, part, count } => {
                let p = *part % parts_n;
                let cursor = kv_index::cursor::encode(p, 0, 0);
                reqs.push((p, scan_req(*db, cursor, *count), cursor));
            }
            c => {
                let ks = c.keys();
                let p = self.keypart[ks[0].0 as usize];
                let keys = ks.iter().map(|(k, a)| (Box::<[u8]>::from(self.keytab[*k as usize].as_slice()), *a)).collect();
                reqs.push((p, Request { db: c.db(), keys, kind: ReqKind::Keyed, origin: Origin::Client }, 0));
            }
        }
        let mut parts = Vec::with_capacity(reqs.len());
        for (i, (p, req, cursor)) in reqs.into_iter().enumerate() {
            let id = self.eng().submit(p, req);
            self.by_req.insert((p, id.0), (uid, i));
            parts.push(PartRun { p, id: Some(id), phase: Phase::Queued, need: Need::Exec, requeues: 0, cursor });
        }
        self.stats.commands += 1;
        self.out.insert(uid, CmdRun { cmd, sink, parts, submitted: self.ticks, step: self.step });
    }

    fn need_for(&self, cmd: &Cmd, txn: bool) -> Need {
        if matches!(cmd, Cmd::Barrier) {
            return Need::Durable;
        }
        match self.cfg.durability {
            Durability::Async if cmd.is_delete() || txn => Need::Written,
            Durability::Async => Need::Exec,
            Durability::Written => Need::Written,
            Durability::Flushed => Need::Durable,
        }
    }

    /// Executes ready request `id` of partition `p`.
    fn exec_part(&mut self, p: u16, id: ReqId) -> Result<(), Failure> {
        let Some(&(uid, pi)) = self.by_req.get(&(p, id.0)) else {
            return Err(Failure::new(FailKind::Engine, "unknown-ready", format!("partition {p} returned ready request {} the driver never submitted (or already finished)", id.0)));
        };
        let (cmd, sink) = {
            let c = &self.out[&uid];
            (c.cmd.clone(), c.sink)
        };
        let op = (uid << 8) | pi as u64;
        match &cmd {
            Cmd::Flush { db } => {
                let st = self.eng().exec(p, id, &mut |_| Ok(()));
                match st {
                    ExecStatus::Done((), out) => {
                        self.by_req.remove(&(p, id.0));
                        let keys: Vec<KeyRef> = self
                            .model
                            .keys()
                            .filter(|((kdb, k), _)| self.keypart[*k as usize] == p && db.is_none_or(|d| d == *kdb))
                            .map(|(k, _)| *k)
                            .collect();
                        for k in &keys {
                            self.model.write(*k, op, false, None);
                        }
                        if self.trace {
                            eprintln!("t{} step {} p{p} op {op} {} part -> ticket {:?} cleared {:?}", self.ticks, self.out[&uid].step, cmd.name(), out.ticket.map(|t| t.0), keys);
                        }
                        self.after_exec(uid, pi, op, out.ticket, true, false)?;
                    }
                    ExecStatus::Requeued => self.requeued(uid, pi),
                    ExecStatus::Failed(e) => self.part_failed(uid, pi, e)?,
                }
            }
            Cmd::Barrier => {
                let st = self.eng().exec(p, id, &mut |_| Ok(()));
                match st {
                    ExecStatus::Done((), out) => {
                        self.by_req.remove(&(p, id.0));
                        self.after_exec(uid, pi, op, out.ticket, false, false)?;
                    }
                    ExecStatus::Requeued => self.requeued(uid, pi),
                    ExecStatus::Failed(e) => self.part_failed(uid, pi, e)?,
                }
            }
            Cmd::Scan { db, .. } => {
                let mut found = Vec::new();
                let st = self.eng().scan(p, id, &|_| true, &mut found);
                match st {
                    ExecStatus::Done(next, _) => {
                        self.by_req.remove(&(p, id.0));
                        for k in &found {
                            let Some(&ki) = self.bykey.get(&k[..]) else {
                                return Err(Failure::new(FailKind::I2, "scan-unknown", format!("SCAN of partition {p} db {db} returned a key that was never written ({} bytes)", k.len())));
                            };
                            if self.keypart[ki as usize] != p {
                                return Err(Failure::new(FailKind::Result, "scan-partition", format!("SCAN of partition {p} returned key {ki} of partition {}", self.keypart[ki as usize])));
                            }
                            match sink {
                                Sink::ScanAll => {
                                    self.scanned.insert(ki);
                                }
                                _ => {
                                    if self.model.key((*db, ki)).is_none_or(|km| km.cur.is_none()) {
                                        return Err(Failure::new(FailKind::Result, "scan-deleted", format!("SCAN of partition {p} db {db} returned key {ki}, which is deleted")));
                                    }
                                }
                            }
                        }
                        let (np, _, _) = kv_index::cursor::decode(next);
                        if np == p && next != 0 {
                            let count = match cmd {
                                Cmd::Scan { count, .. } => count,
                                _ => 10,
                            };
                            let nid = self.eng().submit(p, scan_req(*db, next, count));
                            self.by_req.insert((p, nid.0), (uid, pi));
                            let part = &mut self.out.get_mut(&uid).expect("live").parts[pi];
                            part.id = Some(nid);
                            part.cursor = next;
                        } else {
                            self.after_exec(uid, pi, op, None, false, false)?;
                        }
                    }
                    ExecStatus::Requeued => self.requeued(uid, pi),
                    ExecStatus::Failed(e) => self.part_failed(uid, pi, e)?,
                }
            }
            _ if sink == Sink::Probe => {
                let (db, k) = match cmd {
                    Cmd::Get { db, k } => (db, k),
                    _ => unreachable!("probes are GETs"),
                };
                let key = self.keytab[k as usize].clone();
                let mut got: Val = None;
                let st = self.eng().exec(p, id, &mut |ks| {
                    ks.select(db);
                    got = ks.get(&key)?.map(|(m, v)| (v.to_vec(), m.expire_at_ms));
                    Ok(())
                });
                match st {
                    ExecStatus::Done((), _) => {
                        self.by_req.remove(&(p, id.0));
                        self.probes.insert(uid, got);
                        self.after_exec(uid, pi, op, None, false, false)?;
                    }
                    ExecStatus::Requeued => self.requeued(uid, pi),
                    ExecStatus::Failed(e) => self.part_failed(uid, pi, e)?,
                }
            }
            _ => {
                let st = {
                    let eng = self.eng.as_mut().expect("engine open");
                    let keytab = &self.keytab;
                    eng.exec(p, id, &mut |ks| apply_cmd(&mut StoreKv { ks, keytab }, &cmd, uid))
                };
                match st {
                    ExecStatus::Done(got, out) => {
                        self.by_req.remove(&(p, id.0));
                        let mut mk = ModelKv { model: &self.model, now: self.now_ms, staged: Vec::new() };
                        let want = apply_cmd(&mut mk, &cmd, uid);
                        let writes = mk.staged;
                        if want.as_ref() != Ok(&got) {
                            return Err(Failure::new(
                                FailKind::Result,
                                format!("{}-result", cmd.name()),
                                format!("{} (step {}) observed {:?}, the model expects {:?}", Step::Cmd(cmd.clone()).to_text(), self.out[&uid].step, got, want),
                            ));
                        }
                        let txn = writes.len() >= 2;
                        if self.trace {
                            let w: Vec<String> = writes
                                .iter()
                                .map(|((db, k), v)| match v {
                                    None => format!("{db}/{k}=absent"),
                                    Some((b, at)) => format!("{db}/{k}={}b:{:x}:{:?}", b.len(), crate::model::fingerprint(b) & 0xFFFF, at),
                                })
                                .collect();
                            eprintln!("t{} step {} p{p} op {op} {} -> {:?} ticket {:?} writes [{}]", self.ticks, self.out[&uid].step, Step::Cmd(cmd.clone()).to_text(), got, out.ticket.map(|t| t.0), w.join(" "));
                        }
                        for (k, v) in writes.iter() {
                            self.model.write(*k, op, txn, v.clone());
                        }
                        if !writes.is_empty() && out.ticket.is_none() {
                            return Err(Failure::new(FailKind::Engine, "no-ticket", format!("{} executed {} writes without a ticket", cmd.name(), writes.len())));
                        }
                        self.after_exec(uid, pi, op, out.ticket, !writes.is_empty(), txn)?;
                    }
                    ExecStatus::Requeued => self.requeued(uid, pi),
                    ExecStatus::Failed(e) => self.part_failed(uid, pi, e)?,
                }
            }
        }
        Ok(())
    }

    fn requeued(&mut self, uid: u64, pi: usize) {
        self.stats.requeues += 1;
        if let Some(c) = self.out.get_mut(&uid) {
            c.parts[pi].requeues += 1;
        }
    }

    fn part_failed(&mut self, uid: u64, pi: usize, e: StoreError) -> Result<(), Failure> {
        let (p, id, name) = {
            let c = &self.out[&uid];
            (c.parts[pi].p, c.parts[pi].id, c.cmd.name())
        };
        if let Some(id) = id {
            self.by_req.remove(&(p, id.0));
        }
        match e {
            StoreError::OutOfSpace | StoreError::TooLarge => self.stats.out_of_space += 1,
            // injected device errors: an error reply is a terminal state
            StoreError::Io(_) | StoreError::ReadOnly | StoreError::Corrupt(_) if self.cfg.error_ppm > 0 => self.stats.other_errors += 1,
            StoreError::Corrupt(pg) => {
                return Err(Failure::new(FailKind::Engine, format!("{name}-corrupt"), format!("{name} on partition {p} failed: corrupt page {:#x}", pg.0)));
            }
            other => {
                return Err(Failure::new(FailKind::Engine, format!("{name}-error"), format!("{name} on partition {p} failed: {} (no I/O errors are injected)", fmt_store_err(other))));
            }
        }
        if self.out[&uid].sink == Sink::Probe {
            self.probe_failed.insert(uid);
        }
        let c = self.out.get_mut(&uid).expect("live");
        c.parts[pi].phase = Phase::Done;
        c.parts[pi].id = None;
        self.finish_if_done(uid);
        Ok(())
    }

    /// A part executed: it waits for its ticket or is done.
    fn after_exec(&mut self, uid: u64, pi: usize, op: u64, ticket: Option<TicketId>, wrote: bool, txn: bool) -> Result<(), Failure> {
        let (p, cmd) = {
            let c = &self.out[&uid];
            (c.parts[pi].p, c.cmd.clone())
        };
        let need = match ticket {
            None => Need::Exec,
            Some(_) if self.out[&uid].sink != Sink::Model => Need::Exec,
            Some(_) if !wrote && !matches!(cmd, Cmd::Barrier | Cmd::Flush { .. }) => Need::Exec,
            Some(_) => self.need_for(&cmd, txn),
        };
        if let Some(t) = ticket {
            self.tickets.push(OpenTicket { p, t, op, since: self.ticks, cmd: cmd.name() });
        }
        let c = self.out.get_mut(&uid).expect("live");
        let part = &mut c.parts[pi];
        part.id = None;
        part.need = need;
        part.phase = match (need, ticket) {
            (Need::Exec, _) | (_, None) => Phase::Done,
            (_, Some(t)) => Phase::Ticket(t),
        };
        self.finish_if_done(uid);
        Ok(())
    }

    fn finish_if_done(&mut self, uid: u64) {
        let done = self.out.get(&uid).is_some_and(|c| c.parts.iter().all(|p| p.phase == Phase::Done));
        if done {
            let c = self.out.remove(&uid).expect("live");
            self.stats.max_latency = self.stats.max_latency.max(self.ticks - c.submitted);
        }
    }

    /// Ticket states: model acks, released replies.
    fn observe(&mut self) -> Result<(), Failure> {
        let eng = self.eng.as_ref().expect("engine open");
        let mut i = 0;
        while i < self.tickets.len() {
            let t = &self.tickets[i];
            match eng.ticket_state(t.p, t.t) {
                Some(TicketState::Written) => {
                    self.model.ack(t.op, Ack::Written);
                    i += 1;
                }
                Some(TicketState::Durable) => {
                    if self.trace {
                        eprintln!("t{} op {} ticket {} durable", self.ticks, t.op, t.t.0);
                    }
                    self.model.ack(t.op, Ack::Durable);
                    self.tickets.swap_remove(i);
                }
                Some(TicketState::Failed(e)) => {
                    if self.cfg.error_ppm == 0 {
                        return Err(Failure::new(FailKind::Engine, format!("{}-ticket-failed", t.cmd), format!("ticket of {} on partition {} failed with {e} (no I/O errors are injected)", t.cmd, t.p)));
                    }
                    // the write may be lost: whatever it acknowledged before no longer binds
                    if self.trace {
                        eprintln!("t{} op {} ticket {} failed {e}", self.ticks, t.op, t.t.0);
                    }
                    self.model.unack(t.op);
                    self.stats.other_errors += 1;
                    self.tickets.swap_remove(i);
                }
                _ => i += 1,
            }
        }
        let mut finished = Vec::new();
        for (uid, c) in self.out.iter_mut() {
            let mut all = true;
            for part in c.parts.iter_mut() {
                if let Phase::Ticket(t) = part.phase {
                    let st = eng.ticket_state(part.p, t);
                    let ok = match st {
                        Some(TicketState::Durable) | Some(TicketState::Failed(_)) => true,
                        Some(TicketState::Written) => part.need == Need::Written,
                        _ => false,
                    };
                    if ok {
                        part.phase = Phase::Done;
                    }
                }
                all &= part.phase == Phase::Done;
            }
            if all {
                finished.push(*uid);
            }
        }
        for uid in finished {
            self.finish_if_done(uid);
        }
        Ok(())
    }

    /// One driver iteration: execute every ready request, pump the engine, read tickets.
    fn tick(&mut self) -> Result<(), Failure> {
        let mut ready = Vec::new();
        self.eng().ready(&mut ready);
        if self.cfg.shuffle {
            self.rng.shuffle(&mut ready);
        }
        for (p, id) in ready {
            self.exec_part(p, id)?;
        }
        self.eng().pump();
        self.ticks += 1;
        if !self.frozen_clock {
            self.advance(u64::from(self.cfg.tick_ms));
        }
        self.observe()
    }

    fn io_outstanding(&self) -> usize {
        let Some(e) = self.eng.as_ref() else { return 0 };
        (0..self.cfg.devices as usize).map(|i| e.device(i).outstanding()).sum()
    }

    // -----------------------------------------------------------------------------------------
    // liveness

    /// I15: ticks with no new input until no request is outstanding.
    fn drain(&mut self, what: &str) -> Result<(), Failure> {
        self.drain_from = self.ctl.borrow().delivered;
        for _ in 0..self.cfg.hang_ticks {
            if self.out.is_empty() {
                return Ok(());
            }
            self.tick()?;
        }
        if self.out.is_empty() {
            return Ok(());
        }
        Err(self.hang_report(what))
    }

    fn hang_report(&mut self, what: &str) -> Failure {
        let eng = self.eng.as_ref().expect("engine open");
        let busy = self.ctl.borrow().delivered - self.drain_from;
        let mut detail = format!(
            "{} request(s) outstanding after {} ticks with no new input ({what}); io outstanding {}, {busy} completions meanwhile ({}); seed {}\n",
            self.out.len(),
            self.cfg.hang_ticks,
            self.io_outstanding(),
            if busy > 1000 { "the engine keeps issuing I/O" } else if busy > 0 { "background I/O only" } else { "no I/O at all" },
            self.cfg.seed
        );
        let mut sig = String::new();
        let mut parts_seen = BTreeSet::new();
        for (n, (uid, c)) in self.out.iter().enumerate() {
            for part in &c.parts {
                if part.phase == Phase::Done {
                    continue;
                }
                let wait = match part.phase {
                    Phase::Queued if part.requeues > 0 => format!("re-queued {} time(s), not ready again", part.requeues),
                    Phase::Queued => "submitted, never ready".to_string(),
                    Phase::Ticket(t) => format!("executed, ticket {} is {:?}, reply needs {:?}", t.0, eng.ticket_state(part.p, t), part.need),
                    Phase::Done => unreachable!(),
                };
                if sig.is_empty() {
                    let ph = match part.phase {
                        Phase::Queued if part.requeues > 0 => "requeued".to_string(),
                        Phase::Queued => "never-ready".to_string(),
                        Phase::Ticket(t) => format!("ticket-{:?}-needs-{:?}", eng.ticket_state(part.p, t).unwrap_or(TicketState::Executed), part.need),
                        Phase::Done => String::new(),
                    };
                    sig = format!("{} {ph}", c.cmd.name());
                }
                if n < 12 {
                    let _ = writeln!(
                        detail,
                        "  {} (step {}, uid {uid}) partition {}: {wait}; outstanding {} ticks",
                        Step::Cmd(c.cmd.clone()).to_text(),
                        c.step,
                        part.p,
                        self.ticks - c.submitted
                    );
                }
                parts_seen.insert(part.p);
            }
        }
        for p in parts_seen.iter().take(4) {
            let _ = writeln!(detail, "  {}", eng.partition(*p).diag_deep(eng.cache()));
        }
        for i in 0..self.cfg.devices as usize {
            let _ = writeln!(detail, "  device {i}: {}", eng.device(i).describe());
        }
        let mut f = Failure::new(FailKind::Hang, sig, detail);
        f.step = self.step;
        f
    }

    /// Every executed write durable, every partition quiescent (after `drain`).
    fn settle(&mut self, what: &str) -> Result<(), Failure> {
        let mut quiet = false;
        for _ in 0..self.cfg.settle_ticks {
            if self.tickets.is_empty() && self.out.is_empty() {
                let e = self.eng.as_ref().expect("engine open");
                quiet = (0..e.partitions()).all(|p| e.partition(p).is_quiescent() && e.partition(p).stale_copies() == 0);
                if quiet {
                    return Ok(());
                }
            }
            self.tick()?;
        }
        let eng = self.eng.as_ref().expect("engine open");
        if !self.tickets.is_empty() {
            let mut detail = format!("{} executed write(s) never became durable within {} ticks ({what}); io outstanding {}\n", self.tickets.len(), self.cfg.settle_ticks, self.io_outstanding());
            let mut parts = BTreeSet::new();
            for t in self.tickets.iter().take(8) {
                let _ = writeln!(detail, "  {} ticket {} on partition {}: {:?} for {} ticks", t.cmd, t.t.0, t.p, eng.ticket_state(t.p, t.t), self.ticks - t.since);
                parts.insert(t.p);
            }
            for p in parts.iter().take(4) {
                let _ = writeln!(detail, "  {}", eng.partition(*p).diag_deep(eng.cache()));
            }
            let t = &self.tickets[0];
            let sig = format!("{} ticket {:?}", t.cmd, eng.ticket_state(t.p, t.t).unwrap_or(TicketState::Executed));
            let mut f = Failure::new(FailKind::Stall, sig, detail);
            f.step = self.step;
            return Err(f);
        }
        if !quiet && self.cfg.error_ppm == 0 {
            let mut detail = format!("partitions not quiescent after {} ticks with nothing outstanding ({what})\n", self.cfg.settle_ticks);
            let mut sig = String::new();
            for p in 0..eng.partitions() {
                let part = eng.partition(p);
                if !(part.is_quiescent() && part.stale_copies() == 0) {
                    if sig.is_empty() {
                        sig = if part.is_quiescent() { "stale-copies".into() } else { "busy".into() };
                    }
                    let _ = writeln!(detail, "  {}", part.diag_deep(eng.cache()));
                }
            }
            let mut f = Failure::new(FailKind::NoQuiesce, sig, detail);
            f.step = self.step;
            return Err(f);
        }
        Ok(())
    }

    fn quiesce(&mut self, what: &str) -> Result<(), Failure> {
        self.drain(what)?;
        self.settle(what)?;
        self.stats.quiesces += 1;
        self.verify(Loss::Nothing, what)
    }

    // -----------------------------------------------------------------------------------------
    // verification

    /// Reads every model key (verification GETs, bounded like any request).
    /// GETs of `keys`; also returns the keys whose GET failed (injected errors).
    fn probe_all(&mut self, keys: &[KeyRef], what: &str) -> Result<(Vec<Val>, BTreeSet<KeyRef>), Failure> {
        let mut uids = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(256) {
            for &(db, k) in chunk {
                let uid = self.next_probe;
                self.next_probe += 1;
                self.submit(uid, Cmd::Get { db, k }, Sink::Probe);
                uids.push(uid);
            }
            self.drain(what)?;
        }
        let failed: BTreeSet<KeyRef> = keys.iter().zip(&uids).filter(|(_, u)| self.probe_failed.remove(u)).map(|(k, _)| *k).collect();
        Ok((uids.iter().map(|u| self.probes.remove(u).unwrap_or(None)).collect(), failed))
    }

    /// SCAN of every partition of `db`.
    fn scan_all(&mut self, db: u8, what: &str) -> Result<BTreeSet<u32>, Failure> {
        self.scanned.clear();
        for p in 0..self.cfg.partitions {
            let uid = self.next_probe;
            self.next_probe += 1;
            let count = 1 + self.rng.below(40) as u32;
            self.submit(uid, Cmd::Scan { db, part: p, count }, Sink::ScanAll);
        }
        self.drain(what)?;
        Ok(core::mem::take(&mut self.scanned))
    }

    /// After a crash of kind `loss` (or at a quiesce point, `Loss::Nothing`): every key against
    /// the model (I1, I2, I3), multi-key atomicity (I4), SCAN == live keys, I10; then the
    /// observed state becomes the model's base.
    fn verify(&mut self, loss: Loss, what: &str) -> Result<(), Failure> {
        // with injected errors a failed write may have left the key old or new; what a read
        // sees then proves nothing about media
        let observe_only = loss == Loss::Nothing && self.cfg.error_ppm > 0;
        let loss = if observe_only { Loss::Process } else { loss };
        self.frozen_clock = true;
        self.set_errors(false);
        let r = self.verify_frozen(loss, what, observe_only);
        self.frozen_clock = false;
        self.set_errors(true);
        r
    }

    fn set_errors(&mut self, on: bool) {
        let mut c = self.ctl.borrow_mut();
        c.errors_on = on && c.error_ppm > 0;
    }

    fn verify_frozen(&mut self, loss: Loss, what: &str, observe_only: bool) -> Result<(), Failure> {
        let mut keys: Vec<KeyRef> = self.model.keys().map(|(k, _)| *k).collect();
        // every key of the table in db 0 is checked, written or not
        for k in 0..self.cfg.keys {
            if self.model.key((0, k)).is_none() {
                keys.push((0, k));
            }
        }
        keys.sort_unstable();
        let (got, unreadable) = self.probe_all(&keys, what)?;
        let now = self.now_ms;
        let mut matches = BTreeMap::new();
        for (k, v) in keys.iter().zip(&got) {
            if unreadable.contains(k) {
                // an error reply (runs with injected errors: a key whose write failed may be
                // unreadable until a restart) observes nothing
                continue;
            }
            let st = v.as_ref().map(|(b, at)| State::of(b, *at));
            match self.model.check(*k, st, loss, now) {
                Ok(m) => {
                    matches.insert(*k, m);
                }
                Err(e) => {
                    let kind = if st.is_some() && self.model.key(*k).is_none_or(|km| km.hist.iter().all(|h| h.state.is_none())) { FailKind::I2 } else { FailKind::I1 };
                    let mut f = Failure::new(kind, format!("{what}-{loss:?}"), format!("db {} key {} after {what} ({loss:?}): {e}", k.0, k.1));
                    f.step = self.step;
                    return Err(f);
                }
            }
        }
        if let Err(e) = self.model.check_txns(&matches, loss) {
            let mut f = Failure::new(FailKind::I4, format!("{what}-{loss:?}"), format!("after {what} ({loss:?}): {e}"));
            f.step = self.step;
            return Err(f);
        }
        let mut dbs: BTreeSet<u8> = keys.iter().map(|k| k.0).collect();
        dbs.insert(0);
        for db in dbs {
            let mut found = self.scan_all(db, what)?;
            // a key whose GET failed (its value unreadable after a failed write) proves
            // nothing either way
            found.retain(|k| !unreadable.contains(&(db, *k)));
            let live: BTreeSet<u32> = keys.iter().zip(&got).filter(|(k, v)| k.0 == db && v.is_some()).map(|(k, _)| k.1).collect();
            if found != live {
                let missing: Vec<u32> = live.difference(&found).copied().take(8).collect();
                let extra: Vec<u32> = found.difference(&live).copied().take(8).collect();
                let mut f = Failure::new(
                    FailKind::I2,
                    format!("scan-{what}"),
                    format!("SCAN of db {db} after {what} ({loss:?}) differs from GET: missing {missing:?}, extra {extra:?}"),
                );
                f.step = self.step;
                return Err(f);
            }
        }
        {
            let e = self.eng.as_ref().expect("engine open");
            for p in 0..e.partitions() {
                let c = e.partition(p).counters();
                if c.unexpected_orphans > 0 {
                    let mut f = Failure::new(FailKind::I10, what.to_string(), format!("partition {p}: kvd_unexpected_orphans = {} after {what}", c.unexpected_orphans));
                    f.step = self.step;
                    return Err(f);
                }
            }
        }
        self.stats.keys_checked += keys.len() as u64;
        for (k, v) in keys.into_iter().zip(got) {
            if unreadable.contains(&k) {
                continue;
            }
            if observe_only {
                self.model.observed(k, v);
            } else if let Some(m) = matches.get(&k).copied() {
                self.model.rebase(k, m, loss, v);
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------------------------
    // crash and restart

    fn crash(&mut self, power: bool, tear: TearMode, after: u32) -> Result<(), Failure> {
        // the crash point: `after` more completions reach the engine, then none
        {
            let mut c = self.ctl.borrow_mut();
            c.freeze_at = Some(c.delivered + u64::from(after));
        }
        for _ in 0..5_000 {
            let frozen = {
                let c = self.ctl.borrow();
                c.freeze_at.is_some_and(|f| c.delivered >= f)
            };
            if frozen || self.io_outstanding() == 0 {
                break;
            }
            self.tick()?;
        }
        // requests outstanding beyond the bound are hangs even though the crash ends them
        if let Some((uid, c)) = self.out.iter().find(|(_, c)| self.ticks - c.submitted > u64::from(self.cfg.hang_ticks)) {
            let _ = (uid, c);
            return Err(self.hang_report("before a crash"));
        }
        if self.trace {
            eprintln!("t{} step {} crash power={power} tear={tear:?} io outstanding {}", self.ticks, self.step, self.io_outstanding());
        }
        let mut devs = self.eng.take().expect("engine open").into_devices();
        for d in &mut devs {
            d.crash(power, tear);
        }
        self.ctl.borrow_mut().freeze_at = None;
        self.reset_requests();
        self.model.crashed();
        self.stats.crashes += 1;
        // a restart takes a while: the clock moves on
        self.now_ms += 1 + self.rng.below(3000);
        self.open(devs)?;
        let loss = if power { Loss::Power } else { Loss::Process };
        let what = if power { "power cut" } else { "kill -9" };
        self.verify(loss, what)?;
        self.set_errors(true);
        self.drain("after recovery")
    }

    fn clean(&mut self) -> Result<(), Failure> {
        self.quiesce("before clean shutdown")?;
        // SHUTDOWN's own writes see no injected errors (a failed shutdown ends the run)
        self.set_errors(false);
        let eng = self.eng.take().expect("engine open");
        let devs = eng.shutdown(true).map_err(|e| Failure::new(FailKind::Recover, "shutdown", format!("clean shutdown: {e}")))?;
        self.reset_requests();
        self.model.crashed();
        self.stats.cleans += 1;
        self.now_ms += 1 + self.rng.below(1000);
        let mode = self.open(devs)?;
        if mode == RecoveryMode::Snapshot {
            self.stats.snapshot_restarts += 1;
        }
        self.verify(Loss::Nothing, "clean restart")?;
        self.set_errors(true);
        Ok(())
    }

    fn reset_requests(&mut self) {
        self.out.clear();
        self.by_req.clear();
        self.tickets.clear();
        self.probes.clear();
        self.probe_failed.clear();
    }

    fn run(&mut self, steps: &[Step]) -> Result<(), Failure> {
        self.set_errors(true);
        for (i, s) in steps.iter().enumerate() {
            self.step = i;
            let r = match s {
                Step::Cmd(c) => {
                    self.submit(i as u64, c.clone(), Sink::Model);
                    Ok(())
                }
                Step::Ticks(n) => (0..*n).try_for_each(|_| self.tick()),
                Step::Clock(ms) => {
                    self.advance(u64::from(*ms));
                    Ok(())
                }
                Step::Quiesce => self.quiesce("quiesce point"),
                Step::Crash { power, tear, after } => self.crash(*power, *tear, *after),
                Step::Clean => self.clean(),
            };
            r.map_err(|mut f| {
                f.step = i;
                f
            })?;
        }
        self.step = steps.len();
        self.quiesce("end of run").and_then(|()| self.media_check()).map_err(|mut f| {
            f.step = steps.len();
            f
        })?;
        self.stats.ticks = self.ticks;
        Ok(())
    }

    /// I11 and I3 on media at the end of a run (everything quiesced, then the device caches
    /// flushed): every live key has exactly one valid copy, every absent key none (an expired
    /// key at most one). Copies below their db's flush cutoff are dead and not counted; chunks
    /// are counted through their chunkmap.
    fn media_check(&mut self) -> Result<(), Failure> {
        if self.cfg.error_ppm > 0 {
            // a failed write leaves its key old or new: no exact count
            return Ok(());
        }
        use kv_storage::extent::{self, EXT_KIND_CHUNK};
        use kv_storage::layout::{AG_PAGES, PAGE, PageId};
        use kv_storage::page::{PageParse, parse_small_page};
        use kv_storage::record::REC_KIND_TXN_DELETE_MARKER;
        let eng = self.eng.take().expect("engine open");
        let boot = eng.boot().clone();
        let parts = eng.partitions();
        let cutoffs: Vec<[u64; 16]> = (0..parts).map(|p| eng.meta().partition_meta(p).map_or([0; 16], |m| m.flush_cutoff)).collect();
        let mut devs = eng.into_devices();
        let crc = boot.record_crc();
        // (db, key) -> [(page, seq, small, txn id, expiry)]
        let mut copies: MediaCopies = BTreeMap::new();
        let mut views = Vec::new();
        for (d, dev) in devs.iter_mut().enumerate() {
            dev.inner.flush_now();
            let img = dev.inner.durable_image();
            let l = boot.layouts()[d];
            let first = l.ag_area_page;
            let end = first + u64::from(l.n_ags) * u64::from(AG_PAGES);
            let mut page = first;
            while page < end {
                let at = page as usize * PAGE;
                let Ok(buf) = <&[u8; PAGE]>::try_from(&img[at..at + PAGE]) else { break };
                if buf[..] == ZERO_PAGE[..] {
                    page += 1;
                    continue;
                }
                let id = PageId((d as u64) << 32 | page);
                let part = u16::from(buf[5]);
                if parse_small_page(buf, id, part, crc, &mut views) == PageParse::Ok {
                    for v in &views {
                        let p = kv_common::key_slot(v.key) % parts;
                        let cut = cutoffs[usize::from(p)].get(usize::from(v.hdr.db)).copied().unwrap_or(0);
                        if v.hdr.seq.0 < cut || v.hdr.kind == REC_KIND_TXN_DELETE_MARKER {
                            continue;
                        }
                        let at = (v.hdr.expire_at_ms != 0).then_some(v.hdr.expire_at_ms);
                        copies.entry((v.hdr.db, v.key.to_vec())).or_default().push((id.0, v.hdr.seq.0, true, v.hdr.txn.unwrap_or(0), at));
                    }
                    page += 1;
                    continue;
                }
                if let Some(h) = extent::parse_extent_header(buf, id) {
                    let n = h.n_pages as usize;
                    if h.n_pages > 0 && at + n * PAGE <= img.len() && h.kind != EXT_KIND_CHUNK && extent::verify_extent(&img[at..at + n * PAGE], &h, crc) {
                        let kl = h.key_len as usize;
                        let key = img[at + 80..at + 80 + kl].to_vec();
                        let p = kv_common::key_slot(&key) % parts;
                        let cut = cutoffs[usize::from(p)].get(usize::from(h.db)).copied().unwrap_or(0);
                        if h.seq.0 >= cut {
                            let at = (h.expire_at_ms != 0).then_some(h.expire_at_ms);
                            copies.entry((h.db, key)).or_default().push((id.0, h.seq.0, false, h.txn, at));
                        }
                        // the run's tail past what the checksum covers may hold later writes
                        // (a dead extent whose run was freed and partly reused)
                        page += u64::from(extent::covered_pages(&img[at..at + n * PAGE], &h));
                        continue;
                    }
                }
                page += 1;
            }
        }
        let now = self.now_ms;
        let mut bad = Vec::new();
        // live keys whose only extra copies are older expired ones elsewhere (harmless for
        // correctness, unbounded space)
        let mut leftovers = 0usize;
        // absent keys with a valid unexpired copy: a crash would bring them back
        let mut resurrectable = 0usize;
        for ((db, key), at) in &copies {
            let Some(&k) = self.bykey.get(key) else {
                bad.push(format!("a key never written ({} bytes) has {} copies on media", key.len(), at.len()));
                continue;
            };
            let live = self.model.cur_live((*db, k), now).is_some();
            // the newest copy decides; superseded copies in its own page wait for that page's
            // next build (two-image) and lose to it in recovery: allowed, at most two (I11)
            let newest = at.iter().max_by_key(|c| c.1).copied().expect("non-empty");
            // an expiry without write (§16.4) removes the index entry and leaves the record's
            // bytes as free space: an older expired copy may stay on media after the key is
            // written again. It loses to every newer copy in recovery and is dead once the
            // durable clock floor passes its deadline (at most one floor interval later); it
            // is not one of the copies the third-copy rule bounds (in its own page either: its
            // bytes are free space there until a placement reuses them)
            let leftover = |c: &(u64, u64, bool, u64, Option<u64>)| c.1 < newest.1 && c.4.is_some_and(|t| t <= now);
            let counted: Vec<_> = at.iter().filter(|c| !leftover(c)).collect();
            let others_elsewhere = counted.iter().any(|c| c.0 != newest.0);
            let newest_expired = newest.4.is_some_and(|t| t <= now);
            let ok = if live {
                !others_elsewhere && counted.len() <= 2 && !newest_expired
            } else {
                // an expired newest copy stays on media as "absent" (§16.2); anything else of
                // a deleted key is a copy a crash could bring back
                newest_expired && !others_elsewhere && counted.len() <= 2
            };
            if !ok {
                if live && !newest_expired && at.iter().all(|c| c.0 == newest.0 || c.4.is_some_and(|t| t <= now)) {
                    leftovers += 1;
                }
                if !live && !newest_expired {
                    resurrectable += 1;
                }
                let what = if live { "live" } else { "absent or expired" };
                bad.push(format!("db {db} key {k} ({what}) has {} valid copies on media: {:?}", at.len(), at.iter().map(|(p, sq, small, txn, x)| format!("{}{p:#x}@seq{sq:#x} txn {txn} deadline {x:?}", if *small { "page " } else { "extent " })).collect::<Vec<_>>()));
            }
        }
        for (db, k) in self.model.live_keys(now) {
            if !copies.contains_key(&(db, self.keytab[k as usize].clone())) {
                bad.push(format!("db {db} key {k} is live but has no valid copy on media (not durable after quiesce)"));
            }
        }
        drop(devs);
        if !bad.is_empty() {
            let n = bad.len();
            bad.truncate(8);
            let sig = if bad.iter().any(|b| b.contains("no valid copy")) {
                "copy-missing"
            } else if resurrectable > 0 {
                "deleted-key-copy"
            } else if leftovers == n {
                "expired-leftover"
            } else {
                "extra-copies"
            };
            return Err(Failure::new(FailKind::I11, sig, format!("{n} key(s) violate the copy bounds after the final quiesce:\n  {}", bad.join("\n  "))));
        }
        Ok(())
    }
}

/// A page of zeros (never-written pages are skipped by one comparison).
static ZERO_PAGE: [u8; 4096] = [0; 4096];

/// Valid copies on media: `(db, key) -> [(page, seq, small, txn id, expiry)]`.
type MediaCopies = BTreeMap<(u8, Vec<u8>), Vec<(u64, u64, bool, u64, Option<u64>)>>;

fn scan_req(db: u8, cursor: u64, count: u32) -> Request {
    Request { db, keys: Default::default(), kind: ReqKind::ScanStep { db, cursor, count, vtype: None }, origin: Origin::Client }
}

/// Runs `steps` under `cfg`.
pub fn run_steps(cfg: &StorageSimCfg, steps: &[Step]) -> Result<RunStats, Failure> {
    let r = Sim::new(cfg).and_then(|mut sim| {
        sim.run(steps)?;
        Ok(sim.stats.clone())
    });
    r.map_err(|mut f| {
        // runs with injected device errors form their own failure classes
        if cfg.error_ppm > 0 {
            f.sig = format!("ioerr {}", f.sig);
        }
        f
    })
}

/// Runs one seed's generated steps; `Err` carries the violated invariant.
pub fn run(cfg: &StorageSimCfg) -> Result<RunStats, Failure> {
    run_steps(cfg, &generate_steps(cfg))
}

/// [`run_steps`] with panics caught (unwinding builds only).
pub fn run_caught(cfg: &StorageSimCfg, steps: &[Step]) -> Result<RunStats, Failure> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_steps(cfg, steps))) {
        Ok(r) => r,
        Err(p) => {
            let msg = p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default();
            Err(Failure::new(FailKind::Panic, "panic", msg))
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Case files and minimization

/// The replayable text of a case: configuration, failure and steps (no device content).
pub fn case_text(cfg: &StorageSimCfg, steps: &[Step], fail: Option<&Failure>, note: &str) -> String {
    let mut out = String::from("kv-sim storage case 1\n");
    let _ = writeln!(out, "seed {}", cfg.seed);
    if !note.is_empty() {
        let _ = writeln!(out, "# {note}");
    }
    let _ = writeln!(out, "# matrix cell: {}", cfg.cell());
    if let Some(f) = fail {
        let _ = writeln!(out, "fail {} {} {}", f.kind.name(), f.step, f.sig.replace(char::is_whitespace, "_"));
        for l in f.detail.lines() {
            let _ = writeln!(out, "# {l}");
        }
    }
    cfg.to_text(&mut out);
    for s in steps {
        let _ = writeln!(out, "step {}", s.to_text());
    }
    out
}

/// Parses a case file: configuration, steps and the recorded failure kind.
pub fn parse_case(text: &str) -> Result<(StorageSimCfg, Vec<Step>, Option<FailKind>), String> {
    let mut lines = text.lines();
    if lines.next().map(str::trim) != Some("kv-sim storage case 1") {
        return Err("not a kv-sim storage case".into());
    }
    let mut cfg: Option<StorageSimCfg> = None;
    let mut steps = Vec::new();
    let mut kind = None;
    for l in lines {
        let l = l.trim();
        if l.is_empty() || l.starts_with('#') {
            continue;
        }
        let (head, rest) = l.split_once(' ').unwrap_or((l, ""));
        match head {
            "seed" => {
                let seed: u64 = rest.trim().parse().map_err(|_| format!("bad seed: {l}"))?;
                let mut c = StorageSimCfg::from_seed(seed, &Filter::default());
                // parameters added after a case was written take their neutral value
                c.error_ppm = 0;
                c.no_flush = false;
                c.max_len = 0;
                c.unit_cap = 0;
                cfg = Some(c);
            }
            "fail" => kind = rest.split_whitespace().next().and_then(FailKind::parse),
            "cfg" => {
                let (k, v) = rest.split_once(' ').ok_or_else(|| format!("bad cfg line: {l}"))?;
                cfg.as_mut().ok_or("cfg before seed")?.set(k.trim(), v.trim())?;
            }
            "step" => steps.push(Step::parse(rest)?),
            _ => return Err(format!("unknown line: {l}")),
        }
    }
    Ok((cfg.ok_or("no seed")?, steps, kind))
}

/// Shrinks `steps` while `test` keeps failing with `kind` (ddmin over the step list, then
/// simplification of single steps). `budget` caps the number of test runs.
pub fn minimize(
    cfg: &StorageSimCfg,
    steps: &[Step],
    kind: FailKind,
    budget: usize,
    test: &mut dyn FnMut(&StorageSimCfg, &[Step]) -> Option<Failure>,
) -> (Vec<Step>, Option<Failure>) {
    let mut runs = 0usize;
    let mut cur: Vec<Step> = steps.to_vec();
    let mut last: Option<Failure> = None;
    let mut check = |cand: &[Step], runs: &mut usize, last: &mut Option<Failure>| -> bool {
        *runs += 1;
        match test(cfg, cand) {
            Some(f) if f.kind == kind => {
                *last = Some(f);
                true
            }
            _ => false,
        }
    };
    // the steps after the detection point are not needed
    if check(&cur.clone(), &mut runs, &mut last) {
        let cut = last.as_ref().map_or(cur.len(), |f| (f.step + 1).min(cur.len()));
        if cut < cur.len() {
            let cand: Vec<Step> = cur[..cut].to_vec();
            if check(&cand, &mut runs, &mut last) {
                cur = cand;
            }
        }
    }
    let mut n = 2usize;
    while cur.len() >= 2 && runs < budget {
        let len = cur.len();
        let chunk = len.div_ceil(n);
        let mut reduced = false;
        let mut i = 0;
        while i < n && runs < budget {
            let lo = i * chunk;
            if lo >= len {
                break;
            }
            let hi = (lo + chunk).min(len);
            let cand: Vec<Step> = cur[..lo].iter().chain(cur[hi..].iter()).cloned().collect();
            if check(&cand, &mut runs, &mut last) {
                cur = cand;
                n = (n - 1).max(2);
                reduced = true;
                break;
            }
            i += 1;
        }
        if !reduced {
            if n >= len {
                break;
            }
            n = (n * 2).min(len);
        }
    }
    // simplify single steps: shorter values, fewer ticks, fewer keys per command
    let mut i = 0;
    while i < cur.len() && runs < budget {
        let simpler: Vec<Step> = match &cur[i] {
            Step::Ticks(n) if *n > 1 => vec![Step::Ticks(n / 2), Step::Ticks(1)],
            Step::Clock(ms) if *ms > 1 => vec![Step::Clock(ms / 2)],
            Step::Crash { power, tear, after } if *after > 0 => {
                vec![Step::Crash { power: *power, tear: *tear, after: after / 2 }, Step::Crash { power: *power, tear: *tear, after: 0 }]
            }
            Step::Cmd(Cmd::Set { db, k, len, ttl, cond, get, keep }) if *len > 16 => {
                vec![Step::Cmd(Cmd::Set { db: *db, k: *k, len: len / 2, ttl: *ttl, cond: *cond, get: *get, keep: *keep })]
            }
            Step::Cmd(Cmd::Mset { db, kv }) if kv.len() > 1 => vec![Step::Cmd(Cmd::Mset { db: *db, kv: kv[..kv.len() - 1].to_vec() })],
            Step::Cmd(Cmd::Msetnx { db, kv }) if kv.len() > 1 => vec![Step::Cmd(Cmd::Msetnx { db: *db, kv: kv[..kv.len() - 1].to_vec() })],
            Step::Cmd(Cmd::Del { db, ks }) if ks.len() > 1 => vec![Step::Cmd(Cmd::Del { db: *db, ks: ks[..1].to_vec() })],
            Step::Cmd(Cmd::Append { db, k, len }) if *len > 16 => vec![Step::Cmd(Cmd::Append { db: *db, k: *k, len: len / 2 })],
            _ => Vec::new(),
        };
        let mut took = false;
        for s in simpler {
            if runs >= budget {
                break;
            }
            let mut cand = cur.clone();
            cand[i] = s;
            if check(&cand, &mut runs, &mut last) {
                cur = cand;
                took = true;
                break;
            }
        }
        if !took {
            i += 1;
        }
    }
    (cur, last)
}

// ---------------------------------------------------------------------------------------------
// Command line (`kv-sim storage ...`)

pub mod cli {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    const USAGE: &str = "usage:
  kv-sim storage --seeds N [--start S] [--mode atomic|two-image|both] [--cache wt|wb|both]
                 [--lba 512|4096|both] [--jobs J] [--scale X] [--out DIR] [--wall-secs T]
                 [--no-minimize] [--min-budget RUNS] [--max-min K]
  kv-sim storage --replay FILE|-      (exit 0: passes, 3: fails; prints the outcome)
  kv-sim storage --show SEED [filters]  (prints the case of a seed without running it)";

    #[derive(Clone)]
    struct Opts {
        seeds: u64,
        start: u64,
        filter: Filter,
        jobs: usize,
        out: PathBuf,
        wall: Duration,
        minimize: bool,
        min_budget: usize,
        max_min: usize,
    }

    fn parse_filter(args: &[String], f: &mut Filter) -> Result<(), String> {
        let mut i = 0;
        while i < args.len() {
            let v = args.get(i + 1).cloned().unwrap_or_default();
            match args[i].as_str() {
                "--mode" => {
                    f.mode = match v.as_str() {
                        "atomic" => Some(PageMode::Atomic),
                        "two-image" => Some(PageMode::TwoImage),
                        "both" => None,
                        _ => return Err(format!("--mode {v}")),
                    }
                }
                "--cache" => {
                    f.cache = match v.as_str() {
                        "wt" => Some(CacheMode::WriteThrough),
                        "wb" => Some(CacheMode::WriteBack),
                        "both" => None,
                        _ => return Err(format!("--cache {v}")),
                    }
                }
                "--lba" => {
                    f.lba = match v.as_str() {
                        "512" => Some(512),
                        "4096" => Some(4096),
                        "both" => None,
                        _ => return Err(format!("--lba {v}")),
                    }
                }
                "--scale" => f.scale = v.parse().map_err(|_| format!("--scale {v}"))?,
                "--smoke" => {
                    f.smoke = true;
                    i += 1;
                    continue;
                }
                "--set" => {
                    let (k, x) = v.split_once('=').ok_or_else(|| format!("--set {v}: expected key=value"))?;
                    f.overrides.push((k.to_string(), x.to_string()));
                }
                _ => {
                    i += 1;
                    continue;
                }
            }
            i += 2;
        }
        Ok(())
    }

    fn opt<T: std::str::FromStr>(args: &[String], name: &str) -> Result<Option<T>, String> {
        match args.iter().position(|a| a == name) {
            None => Ok(None),
            Some(i) => args.get(i + 1).and_then(|v| v.parse().ok()).map(Some).ok_or_else(|| format!("{name} needs a value")),
        }
    }

    /// `<workspace>/target/lv-sim/failures`.
    fn default_out() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/lv-sim/failures")
    }

    /// Entry point: `args` after the program name. Returns the exit code.
    pub fn main(args: Vec<String>) -> i32 {
        match args.first().map(String::as_str) {
            Some("storage") => storage(&args[1..]),
            _ => {
                eprintln!("{USAGE}");
                2
            }
        }
    }

    fn storage(args: &[String]) -> i32 {
        let mut filter = Filter::default();
        if let Err(e) = parse_filter(args, &mut filter) {
            eprintln!("{e}\n{USAGE}");
            return 2;
        }
        if let Some(i) = args.iter().position(|a| a == "--replay") {
            return replay(args.get(i + 1).map(String::as_str).unwrap_or("-"));
        }
        if let Ok(Some(seed)) = opt::<u64>(args, "--show") {
            let cfg = StorageSimCfg::from_seed(seed, &filter);
            print!("{}", case_text(&cfg, &generate_steps(&cfg), None, "generated"));
            return 0;
        }
        let parsed = (|| -> Result<Opts, String> {
            Ok(Opts {
                seeds: opt(args, "--seeds")?.ok_or("--seeds is required")?,
                start: opt(args, "--start")?.unwrap_or(0),
                filter,
                jobs: opt(args, "--jobs")?.unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get().saturating_sub(1).clamp(1, 3))),
                out: opt::<String>(args, "--out")?.map(PathBuf::from).unwrap_or_else(default_out),
                wall: Duration::from_secs(opt(args, "--wall-secs")?.unwrap_or(180)),
                minimize: !args.iter().any(|a| a == "--no-minimize"),
                min_budget: opt(args, "--min-budget")?.unwrap_or(150),
                max_min: opt(args, "--max-min")?.unwrap_or(3),
            })
        })();
        let o = match parsed {
            Ok(o) => o,
            Err(e) => {
                eprintln!("{e}\n{USAGE}");
                return 2;
            }
        };
        if args.iter().any(|a| a == "--child") {
            return child(&o);
        }
        parent(&o, args)
    }

    /// Runs seeds in this process, one result line each (the parent reads them).
    fn child(o: &Opts) -> i32 {
        let stdout = std::io::stdout();
        for seed in o.start..o.start + o.seeds {
            let cfg = StorageSimCfg::from_seed(seed, &o.filter);
            {
                let mut w = stdout.lock();
                let _ = writeln!(w, "start {seed}");
                let _ = w.flush();
            }
            let t = Instant::now();
            let r = run_caught(&cfg, &generate_steps(&cfg));
            let ms = t.elapsed().as_millis();
            let mut w = stdout.lock();
            match r {
                Ok(s) => {
                    let _ = writeln!(w, "ok {seed} {ms} ticks={} cmds={} maxlat={} crashes={} cleans={} snap={} oom={} requeues={}", s.ticks, s.commands, s.max_latency, s.crashes, s.cleans, s.snapshot_restarts, s.out_of_space, s.requeues);
                }
                Err(f) => {
                    let _ = writeln!(w, "fail {seed} {ms} {}", f.line());
                }
            }
            let _ = w.flush();
        }
        let _ = writeln!(stdout.lock(), "done");
        0
    }

    fn read_case(path: &str) -> Result<String, String> {
        let mut text = String::new();
        if path == "-" {
            std::io::stdin().read_to_string(&mut text).map_err(|e| e.to_string())?;
        } else {
            text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        }
        Ok(text)
    }

    /// Runs one case file; prints `ok` or `fail <line>`.
    fn replay(path: &str) -> i32 {
        let text = match read_case(path) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("{e}");
                return 2;
            }
        };
        let (cfg, steps, _) = match parse_case(&text) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("{e}");
                return 2;
            }
        };
        match run_caught(&cfg, &steps) {
            Ok(s) => {
                println!("ok ticks={} cmds={} maxlat={}", s.ticks, s.commands, s.max_latency);
                0
            }
            Err(f) => {
                println!("fail {}", f.line());
                3
            }
        }
    }

    /// One failure seen by the parent.
    struct Seen {
        seed: u64,
        kind: FailKind,
        sig: String,
        line: String,
        cell: String,
    }

    enum Msg {
        Line(usize, String),
        Exit(usize),
    }

    struct Worker {
        child: Child,
        /// Seeds of this worker not reported yet, in order.
        todo: Vec<u64>,
        current: Option<u64>,
        last: Instant,
        stderr: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    fn spawn(i: usize, seeds: &[u64], o: &Opts, raw: &[String], tx: &mpsc::Sender<Msg>) -> std::io::Result<Worker> {
        let exe = std::env::current_exe()?;
        let mut args: Vec<String> = vec!["storage".into(), "--child".into(), "--start".into(), seeds[0].to_string(), "--seeds".into(), seeds.len().to_string()];
        for (p, a) in raw.iter().enumerate() {
            if matches!(a.as_str(), "--mode" | "--cache" | "--lba" | "--scale" | "--set")
                && let Some(v) = raw.get(p + 1)
            {
                args.push(a.clone());
                args.push(v.clone());
            }
        }
        if o.filter.smoke {
            args.push("--smoke".into());
        }
        let mut child = Command::new(exe).args(&args).stdout(Stdio::piped()).stderr(Stdio::piped()).stdin(Stdio::null()).spawn()?;
        let out = child.stdout.take().expect("piped");
        let err = child.stderr.take().expect("piped");
        let txo = tx.clone();
        std::thread::spawn(move || {
            for l in BufReader::new(out).lines() {
                let Ok(l) = l else { break };
                if txo.send(Msg::Line(i, l)).is_err() {
                    return;
                }
            }
            let _ = txo.send(Msg::Exit(i));
        });
        let stderr = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let se = stderr.clone();
        std::thread::spawn(move || {
            for l in BufReader::new(err).lines() {
                let Ok(l) = l else { break };
                let mut v = se.lock().expect("stderr buffer");
                v.push(l);
                if v.len() > 40 {
                    v.remove(0);
                }
            }
        });
        Ok(Worker { child, todo: seeds.to_vec(), current: None, last: Instant::now(), stderr })
    }

    fn parent(o: &Opts, raw: &[String]) -> i32 {
        let t0 = Instant::now();
        let all: Vec<u64> = (o.start..o.start + o.seeds).collect();
        // seeds are dealt in batches; a crashed or stuck child is replaced and resumes after the
        // seed it was running
        let batch = 25usize;
        let mut queue: std::collections::VecDeque<Vec<u64>> = all.chunks(batch).map(<[u64]>::to_vec).collect();
        let (tx, rx) = mpsc::channel();
        let mut workers: Vec<Option<Worker>> = Vec::new();
        let mut passed = 0u64;
        let mut seen: Vec<Seen> = Vec::new();
        let mut cells: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        let mut durs: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        let mut slowest = (0u128, 0u64);
        let mut done = 0u64;
        let total = all.len() as u64;
        let mut last_print = Instant::now();
        let start_worker = |slot: usize, seeds: Vec<u64>, workers: &mut Vec<Option<Worker>>| {
            match spawn(slot, &seeds, o, raw, &tx) {
                Ok(w) => {
                    if slot < workers.len() {
                        workers[slot] = Some(w);
                    } else {
                        workers.push(Some(w));
                    }
                }
                Err(e) => eprintln!("cannot start a worker process: {e}"),
            }
        };
        for slot in 0..o.jobs.max(1) {
            if let Some(b) = queue.pop_front() {
                start_worker(slot, b, &mut workers);
            }
        }
        while workers.iter().any(Option::is_some) {
            let msg = rx.recv_timeout(Duration::from_secs(2));
            match msg {
                Ok(Msg::Line(i, l)) => {
                    let Some(w) = workers.get_mut(i).and_then(Option::as_mut) else { continue };
                    w.last = Instant::now();
                    let mut it = l.splitn(4, ' ');
                    match it.next() {
                        Some("start") => w.current = it.next().and_then(|s| s.parse().ok()),
                        Some("ok") => {
                            let seed: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                            let ms: u128 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                            w.todo.retain(|&s| s != seed);
                            w.current = None;
                            passed += 1;
                            done += 1;
                            if ms > slowest.0 {
                                slowest = (ms, seed);
                            }
                            let c = StorageSimCfg::from_seed(seed, &o.filter);
                            cells.entry(c.matrix()).or_default().0 += 1;
                            durs.entry(format!("{:?}", c.durability)).or_default().0 += 1;
                        }
                        Some("fail") => {
                            let seed: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
                            let _ms = it.next();
                            let rest = it.next().unwrap_or("").to_string();
                            w.todo.retain(|&s| s != seed);
                            w.current = None;
                            done += 1;
                            let kind = rest.split_whitespace().next().and_then(FailKind::parse).unwrap_or(FailKind::Engine);
                            let sig = rest.split_once('[').and_then(|(_, r)| r.split_once(']')).map(|(s, _)| s.to_string()).unwrap_or_default();
                            let c = StorageSimCfg::from_seed(seed, &o.filter);
                            let cell = c.cell();
                            cells.entry(c.matrix()).or_default().1 += 1;
                            durs.entry(format!("{:?}", c.durability)).or_default().1 += 1;
                            println!("FAIL seed {seed} ({cell}): {}", truncate(&rest, 600));
                            seen.push(Seen { seed, kind, sig, line: rest, cell });
                        }
                        Some("done") => {}
                        _ => {}
                    }
                }
                Ok(Msg::Exit(i)) => {
                    let Some(mut w) = workers.get_mut(i).and_then(Option::take) else { continue };
                    let status = w.child.wait().ok();
                    if let Some(seed) = w.current.take() {
                        // the child died inside a seed: a panic (release builds abort)
                        let tail = w.stderr.lock().map(|v| v.join(" | ")).unwrap_or_default();
                        let c = StorageSimCfg::from_seed(seed, &o.filter);
                        let cell = c.cell();
                        let line = format!("PANIC step=? [panic] :: exit {:?}: {}", status, truncate(&tail, 1500));
                        println!("FAIL seed {seed} ({cell}): {line}");
                        cells.entry(c.matrix()).or_default().1 += 1;
                        durs.entry(format!("{:?}", c.durability)).or_default().1 += 1;
                        seen.push(Seen { seed, kind: FailKind::Panic, sig: "panic".into(), line, cell });
                        done += 1;
                        w.todo.retain(|&s| s != seed);
                    }
                    let rest: Vec<u64> = w.todo.clone();
                    if !rest.is_empty() {
                        start_worker(i, rest, &mut workers);
                    } else if let Some(b) = queue.pop_front() {
                        start_worker(i, b, &mut workers);
                    }
                }
                Err(_) => {}
            }
            // wall-clock watchdog
            for i in 0..workers.len() {
                let stuck = workers[i].as_ref().is_some_and(|w| w.current.is_some() && w.last.elapsed() > o.wall);
                if stuck {
                    let mut w = workers[i].take().expect("present");
                    let _ = w.child.kill();
                    let _ = w.child.wait();
                    let seed = w.current.take().expect("current");
                    let c = StorageSimCfg::from_seed(seed, &o.filter);
                    let cell = c.cell();
                    let line = format!("WALL step=? [wall] :: no progress for {} s (a loop inside the engine)", o.wall.as_secs());
                    println!("FAIL seed {seed} ({cell}): {line}");
                    cells.entry(c.matrix()).or_default().1 += 1;
                    durs.entry(format!("{:?}", c.durability)).or_default().1 += 1;
                    seen.push(Seen { seed, kind: FailKind::Wall, sig: "wall".into(), line, cell });
                    done += 1;
                    w.todo.retain(|&s| s != seed);
                    // the reader thread reports the exit; the slot restarts from there
                    let rest = w.todo.clone();
                    if !rest.is_empty() {
                        start_worker(i, rest, &mut workers);
                    } else if let Some(b) = queue.pop_front() {
                        start_worker(i, b, &mut workers);
                    }
                }
            }
            if last_print.elapsed() > Duration::from_secs(30) {
                last_print = Instant::now();
                println!("progress: {done}/{total} seeds, {} failures, {:.0} s", seen.len(), t0.elapsed().as_secs_f64());
            }
        }
        println!();
        println!("== storage simulation: seeds {}..{} ({} runs) in {:.0} s; slowest seed {} ({} ms)", o.start, o.start + o.seeds, done, t0.elapsed().as_secs_f64(), slowest.1, slowest.0);
        println!("   passed {passed}, failed {}", seen.len());
        println!("   per matrix cell (passed / failed):");
        for (c, (ok, bad)) in &cells {
            println!("     {c:<24} {ok:>6} / {bad}");
        }
        println!("   per durability mode (passed / failed):");
        for (c, (ok, bad)) in &durs {
            println!("     {c:<24} {ok:>6} / {bad}");
        }
        let mut by_sig: BTreeMap<(FailKind, String), Vec<&Seen>> = BTreeMap::new();
        for s in &seen {
            by_sig.entry((s.kind, s.sig.clone())).or_default().push(s);
        }
        if !by_sig.is_empty() {
            println!("   failures by class:");
            for ((k, sig), v) in &by_sig {
                let seeds: Vec<String> = v.iter().take(12).map(|s| s.seed.to_string()).collect();
                println!("     {:<9} {:<40} x{:<4} seeds {}", k.name(), sig, v.len(), seeds.join(","));
            }
        }
        if o.minimize && !by_sig.is_empty() {
            println!();
            println!("== minimizing (up to {} per class, {} runs each, {} at a time)", o.max_min, o.min_budget, o.jobs.max(1));
            let work: Vec<(u64, FailKind, String, String, String)> = by_sig
                .iter()
                .flat_map(|((kind, sig), v)| v.iter().take(o.max_min).map(move |s| (s.seed, *kind, sig.clone(), s.cell.clone(), s.line.clone())))
                .collect();
            let next = std::sync::atomic::AtomicUsize::new(0);
            std::thread::scope(|sc| {
                for _ in 0..o.jobs.max(1) {
                    sc.spawn(|| {
                        loop {
                            let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let Some((seed, kind, sig, cell, line)) = work.get(i) else { break };
                            minimize_seed(o, *seed, *kind, sig, cell, line);
                        }
                    });
                }
            });
        }
        if seen.is_empty() { 0 } else { 1 }
    }

    fn truncate(s: &str, n: usize) -> String {
        if s.len() <= n {
            return s.to_string();
        }
        let mut cut = n;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        format!("{}...", &s[..cut])
    }

    /// Runs a case in a fresh process (panics abort release builds; loops are cut by `wall`).
    fn run_isolated(cfg: &StorageSimCfg, steps: &[Step], wall: Duration) -> Option<Failure> {
        let exe = std::env::current_exe().ok()?;
        let mut child = Command::new(exe)
            .args(["storage", "--replay", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .ok()?;
        let text = case_text(cfg, steps, None, "");
        if let Some(mut si) = child.stdin.take() {
            let _ = si.write_all(text.as_bytes());
        }
        let mut out = child.stdout.take().expect("piped");
        let mut err = child.stderr.take().expect("piped");
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut s = String::new();
            let _ = out.read_to_string(&mut s);
            let mut e = String::new();
            let _ = err.read_to_string(&mut e);
            let _ = tx.send((s, e));
        });
        let t = Instant::now();
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    let (s, e) = rx.recv_timeout(Duration::from_secs(5)).unwrap_or_default();
                    return match status.code() {
                        Some(0) => None,
                        Some(3) => {
                            let line = s.lines().find(|l| l.starts_with("fail ")).unwrap_or("fail ENGINE step=0 [] :: ?");
                            Some(parse_line(&line[5..]))
                        }
                        _ => {
                            let tail: Vec<&str> = e.lines().rev().take(12).collect();
                            Some(Failure::new(FailKind::Panic, "panic", tail.into_iter().rev().collect::<Vec<_>>().join(" | ")))
                        }
                    };
                }
                Ok(None) => {
                    if t.elapsed() > wall {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Some(Failure::new(FailKind::Wall, "wall", format!("no result within {} s", wall.as_secs())));
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return None,
            }
        }
    }

    /// Parses `KIND step=N [sig] :: detail`.
    fn parse_line(l: &str) -> Failure {
        let kind = l.split_whitespace().next().and_then(FailKind::parse).unwrap_or(FailKind::Engine);
        let step = l.split("step=").nth(1).and_then(|r| r.split_whitespace().next()).and_then(|s| s.parse().ok()).unwrap_or(0);
        let sig = l.split_once('[').and_then(|(_, r)| r.split_once(']')).map(|(s, _)| s.to_string()).unwrap_or_default();
        let detail = l.split_once(" :: ").map(|(_, d)| d.replace(" | ", "\n")).unwrap_or_default();
        Failure { kind, step, sig, detail }
    }

    fn minimize_seed(o: &Opts, seed: u64, kind: FailKind, sig: &str, cell: &str, line: &str) {
        let cfg = StorageSimCfg::from_seed(seed, &o.filter);
        let steps = generate_steps(&cfg);
        let t = Instant::now();
        let wall = o.wall;
        let (min, last) = minimize(&cfg, &steps, kind, o.min_budget, &mut |c, s| run_isolated(c, s, wall));
        let fail = last.unwrap_or_else(|| parse_line(line));
        let note = format!("minimized from {} to {} steps in {:.0} s ({cell}); original: {}", steps.len(), min.len(), t.elapsed().as_secs_f64(), truncate(line, 300));
        let text = case_text(&cfg, &min, Some(&fail), &note);
        let head = match crate::write_artifact(&o.out, seed, text.as_bytes()) {
            Ok(p) => format!("   {} [{sig}] seed {seed}: {} -> {} steps: {}", kind.name(), steps.len(), min.len(), show(&p)),
            Err(e) => format!("   {} seed {seed}: cannot write the case: {e}", kind.name()),
        };
        // one print per case so that parallel minimizations do not interleave
        println!("{head}\n      {}", truncate(&fail.line(), 400));
    }

    fn show(p: &Path) -> String {
        p.canonicalize().unwrap_or_else(|_| p.to_path_buf()).display().to_string()
    }
}

// ---------------------------------------------------------------------------------------------
// Regression cases

/// Minimized cases of the failures the campaigns found, one per distinct defect (the `fail`
/// line records what the case showed when it was minimized). Each passes once its defect is
/// fixed (the first ones pass already and guard the fixes); `kv-sim storage --replay FILE`
/// runs one from a file.
pub const REGRESSION_CASES: &[(&str, &str)] = &[
    (
        "flush_supersedes_unflushed_write",
        r#"kv-sim storage case 1
seed 1113
fail STALL 2 SET_ticket_Written
cfg partitions 16
cfg devices 1
cfg ags 47
cfg lba 512
cfg page_mode two-image
cfg atomic_unit -
cfg cache wb
cfg dsync 0
cfg durability written
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 0
cfg keys 307
cfg groups 307
cfg dbs 3
cfg profile small
cfg steps 867
cfg rounds 1
cfg tick_permille 120
cfg delay_max 32
cfg late_pct 18
cfg skip_pct 0
cfg per_poll 1
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
step cmd set 0 215 16 - - 0 0
step cmd flush all
"#,
    ),
    (
        "flush_after_extent_setrange_hangs_flushed_reply",
        r#"kv-sim storage case 1
seed 1059
fail HANG 2 SETRANGE_ticket-Written-needs-Durable
cfg partitions 16
cfg devices 1
cfg ags 44
cfg lba 4096
cfg page_mode atomic
cfg atomic_unit 8192
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 67108864
cfg fill_pct 95
cfg snapshot 0
cfg keys 187
cfg groups 62
cfg dbs 3
cfg profile mixed
cfg steps 553
cfg rounds 2
cfg tick_permille 250
cfg delay_max 32
cfg late_pct 6
cfg skip_pct 40
cfg per_poll 2
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
step cmd setrange 2 54 698 438
step cmd flush all
"#,
    ),
    (
        "chunkmap_append_requeued_forever",
        r#"kv-sim storage case 1
seed 1104
fail HANG 2 APPEND_requeued
cfg partitions 64
cfg devices 1
cfg ags 137
cfg lba 4096
cfg page_mode two-image
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 1
cfg keys 17
cfg groups 2
cfg dbs 1
cfg profile large
cfg steps 353
cfg rounds 1
cfg tick_permille 450
cfg delay_max 0
cfg late_pct 32
cfg skip_pct 0
cfg per_poll 0
cfg shuffle 0
cfg tick_ms 2
cfg hang_ticks 20000
cfg settle_ticks 30000
step cmd set 0 1 9284496 r35884696 - 0 0
step cmd append 0 1 11
"#,
    ),
    (
        "chunkmap_get_requeued_forever",
        r#"kv-sim storage case 1
seed 1007
fail HANG 2 GET_requeued
cfg partitions 16
cfg devices 1
cfg ags 52
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 0
cfg keys 20
cfg groups 5
cfg dbs 1
cfg profile large
cfg steps 308
cfg rounds 1
cfg tick_permille 120
cfg delay_max 32
cfg late_pct 30
cfg skip_pct 0
cfg per_poll 2
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
step cmd set 0 1 11598529 r1476 - 0 0
step cmd get 0 1
"#,
    ),
    (
        "atomic_unit_64k_unaligned_extent_rewrite",
        r#"kv-sim storage case 1
seed 1108
fail ENGINE 4 EXPIRE-ticket-failed
cfg partitions 32
cfg devices 1
cfg ags 85
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 65536
cfg cache wt
cfg dsync 0
cfg durability async
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 1
cfg keys 149
cfg groups 24
cfg dbs 3
cfg profile mixed
cfg steps 1432
cfg rounds 1
cfg tick_permille 450
cfg delay_max 32
cfg late_pct 56
cfg skip_pct 10
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
step cmd msetnx 2 11:326,83:717965
step ticks 4
step cmd set 0 59 12932 - - 0 0
step cmd expire 0 59 r3831 0
"#,
    ),
    (
        "page_extent_append_lost_update",
        r#"kv-sim storage case 1
seed 1064
fail RESULT 4 APPEND-result
cfg partitions 32
cfg devices 1
cfg ags 88
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability written
cfg cache_bytes 67108864
cfg fill_pct 95
cfg snapshot 1
cfg keys 271
cfg groups 67
cfg dbs 1
cfg profile mixed
cfg steps 549
cfg rounds 1
cfg tick_permille 40
cfg delay_max 4
cfg late_pct 24
cfg skip_pct 0
cfg per_poll 2
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 1
cfg max_len 8000000
cfg unit_cap 4096
step cmd msetnx 0 4:2052
step ticks 5
step cmd append 0 4 11
step cmd append 0 4 16
"#,
    ),
    (
        "page_extent_setrange_lost_update",
        r#"kv-sim storage case 1
seed 1216
fail I1 4 end_of_run-Nothing
cfg partitions 16
cfg devices 1
cfg ags 52
cfg lba 4096
cfg page_mode atomic
cfg atomic_unit 8192
cfg cache wt
cfg dsync 0
cfg durability written
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 1
cfg keys 53
cfg groups 17
cfg dbs 1
cfg profile churn
cfg steps 1206
cfg rounds 1
cfg tick_permille 120
cfg delay_max 32
cfg late_pct 36
cfg skip_pct 10
cfg per_poll 8
cfg shuffle 1
cfg tick_ms 5
cfg hang_ticks 20000
cfg settle_ticks 30000
step cmd set 0 4 2705 - - 0 0
step ticks 5
step cmd setrange 0 4 816 356
step cmd setrange 0 4 1079 54
"#,
    ),
    (
        "set_after_expired_record_invisible",
        r#"kv-sim storage case 1
seed 1263
fail RESULT 4 EXPIRE-result
cfg partitions 16
cfg devices 1
cfg ags 44
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 8192
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 1
cfg keys 27
cfg groups 27
cfg dbs 2
cfg profile churn
cfg steps 483
cfg rounds 5
cfg tick_permille 250
cfg delay_max 0
cfg late_pct 47
cfg skip_pct 0
cfg per_poll 8
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
step cmd set 0 1 2472 past - 0 0
step ticks 4
step cmd set 0 1 3222 - - 0 0
step cmd expire 0 1 r21318236 1
"#,
    ),
    (
        "delete_of_page_extent_never_durable",
        r#"kv-sim storage case 1
seed 1114
fail HANG 4 DEL_ticket-Executed-needs-Written
cfg partitions 16
cfg devices 1
cfg ags 38
cfg lba 4096
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability async
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 0
cfg keys 60
cfg groups 60
cfg dbs 2
cfg profile full
cfg steps 1056
cfg rounds 2
cfg tick_permille 120
cfg delay_max 4
cfg late_pct 8
cfg skip_pct 40
cfg per_poll 2
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 1
cfg max_len 8000000
cfg unit_cap 4096
step cmd set 0 3 2174 r43754996 - 0 0
step cmd del 0 3
step cmd msetnx 0 3:21531
step cmd del 0 3
"#,
    ),
    (
        "delete_after_extent_mset_never_durable",
        r#"kv-sim storage case 1
seed 1126
fail HANG 5 DEL_ticket-Executed-needs-Durable
cfg partitions 64
cfg devices 1
cfg ags 132
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 93
cfg snapshot 1
cfg keys 32
cfg groups 32
cfg dbs 1
cfg profile full
cfg steps 1025
cfg rounds 3
cfg tick_permille 40
cfg delay_max 32
cfg late_pct 48
cfg skip_pct 10
cfg per_poll 1
cfg shuffle 1
cfg tick_ms 2
cfg hang_ticks 20000
cfg settle_ticks 30000
step cmd setrange 0 1 212 193
step cmd mset 0 0:278,0:149401
step cmd del 0 0
step cmd set 0 0 2418 - - 0 0
step cmd del 0 0
"#,
    ),
    (
        "barrier_after_append_del_livelock",
        r#"kv-sim storage case 1
seed 1067
fail HANG 10 BARRIER_ticket-Executed-needs-Durable
cfg partitions 16
cfg devices 1
cfg ags 44
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability written
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 0
cfg keys 15
cfg groups 2
cfg dbs 2
cfg profile churn
cfg steps 1042
cfg rounds 3
cfg tick_permille 250
cfg delay_max 200
cfg late_pct 14
cfg skip_pct 0
cfg per_poll 1
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
step ticks 1
step cmd append 0 0 2060
step cmd set 0 4 410 - - 1 0
step cmd append 0 0 69
step cmd del 0 0,0,4
step cmd barrier
step cmd mset 1 9:3166,1:1098,11:769
step cmd set 0 0 1927 r192 - 0 0
step cmd set 0 0 2070 - - 0 1
step cmd scan 0 6 38
"#,
    ),
    (
        "deferred_page_never_built",
        r#"kv-sim storage case 1
seed 1181
fail NOQUIESCE 16 busy
cfg partitions 16
cfg devices 1
cfg ags 37
cfg lba 512
cfg page_mode two-image
cfg atomic_unit -
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 67108864
cfg fill_pct 95
cfg snapshot 1
cfg keys 89
cfg groups 17
cfg dbs 1
cfg profile mixed
cfg steps 1272
cfg rounds 1
cfg tick_permille 40
cfg delay_max 4
cfg late_pct 26
cfg skip_pct 0
cfg per_poll 2
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
step cmd mset 0 74:9,57:894,6:34637
step cmd set 0 0 1137 - - 0 0
step cmd del 0 14,82
step cmd mset 0 5:1032,22:14,39:208,73:1421
step cmd expire 0 9 r3689 1
step cmd set 0 33 1470 r163 - 0 1
step cmd set 0 6 357146 - xx 0 0
step cmd get 0 83
step cmd persist 0 8
step cmd meta 0 0
step cmd set 0 6 24 - - 0 0
step cmd get 0 56
step cmd flush 0
step cmd set 0 62 351 - nx 0 0
step cmd set 0 1 381 - - 0 1
step cmd set 0 6 913 - - 0 1
"#,
    ),
    (
        "io_error_readonly_barrier_hang",
        r#"kv-sim storage case 1
seed 1050
fail HANG 7 BARRIER_ticket-Executed-needs-Durable
cfg partitions 16
cfg devices 1
cfg ags 47
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 0
cfg keys 69
cfg groups 11
cfg dbs 2
cfg profile small
cfg steps 1049
cfg rounds 2
cfg tick_permille 120
cfg delay_max 4
cfg late_pct 41
cfg skip_pct 0
cfg per_poll 8
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 20000
cfg no_flush 1
cfg max_len 8000000
cfg unit_cap 4096
step cmd mset 0 27:29,38:406,49:561,60:31
step cmd set 1 0 16 r187 - 0 0
step cmd set 0 34 15 - - 0 0
step cmd set 0 8 15 past nx 0 1
step ticks 4
step cmd mset 0 15:1153,48:1499,4:1382,26:1107
step cmd barrier
"#,
    ),
    (
        "multi_key_delete_partial_after_kill",
        r#"kv-sim storage case 1
seed 1037
fail I4 11 kill_-9-Process
cfg partitions 32
cfg devices 2
cfg ags 45
cfg lba 4096
cfg page_mode two-image
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability async
cfg cache_bytes 16777216
cfg fill_pct 95
cfg snapshot 1
cfg keys 13
cfg groups 4
cfg dbs 1
cfg profile large
cfg steps 239
cfg rounds 4
cfg tick_permille 120
cfg delay_max 4
cfg late_pct 24
cfg skip_pct 0
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 2
cfg hang_ticks 20000
cfg settle_ticks 30000
step cmd mset 0 0:6394893,0:2777,0:722387
step cmd copy 0 0 0 0
step cmd get 0 0
step cmd rename 0 0 8 0
step cmd set 0 0 9152265 - nx 0 0
step cmd del 0 8,0
step cmd set 0 1 138130 r74449385 - 0 0
step ticks 2
step ticks 2
step ticks 7
step ticks 6
step crash kill none 141
"#,
    ),
    (
        "acked_write_lost_after_power_cut",
        r#"kv-sim storage case 1
seed 1133
fail I1 8 power_cut-Power
cfg partitions 16
cfg devices 2
cfg ags 21
cfg lba 4096
cfg page_mode two-image
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 0
cfg keys 32
cfg groups 16
cfg dbs 3
cfg profile mixed
cfg steps 1305
cfg rounds 5
cfg tick_permille 120
cfg delay_max 32
cfg late_pct 30
cfg skip_pct 0
cfg per_poll 8
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 1
cfg max_len 8000000
cfg unit_cap 4096
step cmd set 0 18 2499 - - 0 0
step cmd set 0 28 2882 - - 0 0
step cmd mset 0 29:244,13:915
step ticks 4
step cmd set 0 3 3933 - - 0 0
step cmd set 0 3 6278 r3625 - 0 0
step ticks 2
step ticks 1
step crash power sectors 0
"#,
    ),
    (
        "orphans_after_clean_restart",
        r#"kv-sim storage case 1
seed 1025
fail I10 30 clean_restart
cfg partitions 16
cfg devices 2
cfg ags 21
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability written
cfg cache_bytes 67108864
cfg fill_pct 95
cfg snapshot 1
cfg keys 106
cfg groups 35
cfg dbs 1
cfg profile mixed
cfg steps 1291
cfg rounds 5
cfg tick_permille 120
cfg delay_max 200
cfg late_pct 44
cfg skip_pct 40
cfg per_poll 2
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 1
cfg max_len 8000000
cfg unit_cap 4096
step cmd get 0 9
step cmd set 0 82 833699 r70756591 - 0 0
step cmd msetnx 0 1:26,1:1141
step cmd get 0 12
step cmd set 0 44 8000000 - - 0 0
step cmd expire 0 34 r186 1
step cmd set 0 1 347 r138 - 0 0
step ticks 1
step cmd set 0 94 31269 past - 0 0
step cmd del 0 15
step cmd del 0 38
step cmd set 0 10 1465 past nx 0 1
step cmd mset 0 10:28,10:919,45:1063
step cmd get 0 41
step cmd del 0 43
step quiesce
step cmd persist 0 10
step ticks 3
step cmd get 0 77
step cmd set 0 10 25 - xx 0 1
step cmd del 0 72
step ticks 4
step cmd getdel 0 46
step cmd mset 0 35:96,35:944,35:327
step cmd set 0 0 192 r168 xx 0 0
step cmd set 0 1 231 - - 0 0
step cmd append 0 37 155
step ticks 43
step cmd get 0 17
step cmd expire 0 32 - 0
step clean
step cmd expire 0 8 r142 0
"#,
    ),
    (
        "io_error_open_txn_barrier_hang",
        r#"kv-sim storage case 1
seed 50095
fail HANG 3 ioerr_BARRIER_ticket-Executed-needs-Durable
cfg partitions 16
cfg devices 1
cfg ags 57
cfg lba 512
cfg page_mode two-image
cfg atomic_unit -
cfg cache wb
cfg dsync 0
cfg durability written
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 1
cfg keys 177
cfg groups 59
cfg dbs 3
cfg profile mixed
cfg steps 347
cfg rounds 5
cfg tick_permille 120
cfg delay_max 200
cfg late_pct 15
cfg skip_pct 40
cfg per_poll 8
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 20000
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd mset 0 161:95,161:970,43:942
step cmd barrier
step cmd set 1 19 11 - - 1 0
"#,
    ),
    (
        "large_mset_txn_held_deferred_page_hang",
        r#"kv-sim storage case 1
seed 50874
fail HANG 40 SET_ticket-Executed-needs-Written
cfg partitions 16
cfg devices 1
cfg ags 67
cfg lba 512
cfg page_mode two-image
cfg atomic_unit 512
cfg cache wb
cfg dsync 0
cfg durability written
cfg cache_bytes 67108864
cfg fill_pct 95
cfg snapshot 1
cfg keys 10
cfg groups 2
cfg dbs 3
cfg profile large
cfg steps 479
cfg rounds 2
cfg tick_permille 120
cfg delay_max 4
cfg late_pct 38
cfg skip_pct 10
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd mset 0 3:9731680,3:405,1:4491750,3:3076
step cmd meta 2 9
step cmd get 0 0
step cmd set 0 2 1532850 - - 0 0
step ticks 4
step cmd mset 0 0:3203,0:56193,2:8410
step cmd set 2 8 49065 - - 0 0
step cmd del 2 0,0
step cmd setrange 0 7 63 45
step cmd del 0 2,6
step cmd barrier
step cmd set 1 0 8325248 - - 0 0
step ticks 7
step cmd setrange 0 2 339 571
step cmd rename 0 0 8 0
step cmd msetnx 0 0:24004,6:1096,2:2792364
step clock 19
step cmd scan 0 14 8
step cmd set 0 7 179 - - 0 0
step cmd persist 0 3
step ticks 8
step cmd get 0 0
step cmd setrange 0 0 529 446
step cmd set 0 0 28492 r141 - 0 0
step cmd get 0 7
step cmd scan 0 10 27
step cmd expire 0 3 r180 1
step cmd set 0 0 448 - xx 0 0
step cmd mset 0 9:39988,7:65,1:3212
step cmd set 1 0 833258 r4336 - 0 0
step cmd set 0 0 5964259 - nx 0 0
step cmd get 0 9
step cmd set 1 0 832 - - 0 0
step ticks 8
step cmd set 2 0 363 r2858 - 1 0
step cmd get 0 0
step cmd set 2 5 56945 r4574 - 0 0
step cmd mset 0 0:954,2:614476,6:129419,2:3444
step cmd append 0 1 200
step cmd persist 0 0
"#,
    ),
    (
        "delete_behind_rename_never_durable",
        r#"kv-sim storage case 1
seed 51224
fail HANG 151 DEL_ticket-Executed-needs-Durable
cfg partitions 32
cfg devices 2
cfg ags 42
cfg lba 4096
cfg page_mode two-image
cfg atomic_unit -
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 1
cfg keys 21
cfg groups 5
cfg dbs 1
cfg profile churn
cfg steps 1425
cfg rounds 2
cfg tick_permille 120
cfg delay_max 200
cfg late_pct 13
cfg skip_pct 40
cfg per_poll 0
cfg shuffle 0
cfg tick_ms 2
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step ticks 8
step cmd set 0 20 17407 - - 0 0
step cmd set 0 1 565 - - 0 0
step ticks 3
step cmd get 0 1
step cmd rename 0 0 0 1
step cmd meta 0 0
step cmd meta 0 7
step cmd expire 0 13 - 0
step cmd del 0 11,16,16
step cmd append 0 10 144
step cmd set 0 1 18 - xx 0 0
step cmd set 0 12 2051 r62463711 xx 0 0
step cmd set 0 0 538 r193 - 0 0
step cmd set 0 4 24709 - nx 0 0
step ticks 5
step cmd set 0 16 1356 r73163376 nx 0 0
step ticks 1
step cmd get 0 0
step cmd barrier
step ticks 6
step cmd del 0 13,8,13
step cmd expire 0 18 - 1
step ticks 8
step cmd getdel 0 17
step cmd set 0 0 2768 - - 0 0
step cmd getdel 0 20
step cmd del 0 5
step cmd get 0 4
step cmd append 0 1 99
step cmd msetnx 0 2:1920,2:659,12:17
step cmd meta 0 1
step cmd rename 0 1 6 0
step cmd del 0 1,6
step cmd set 0 1 40061 - - 1 0
step cmd set 0 1 3941 - xx 0 0
step cmd expire 0 8 r35 0
step cmd del 0 19
step ticks 8
step cmd get 0 4
step cmd del 0 1,6,16
step ticks 47
step cmd get 0 1
step cmd rename 0 19 9 0
step cmd set 0 5 28 r175 - 0 0
step cmd mset 0 1:178,11:380
step cmd msetnx 0 0:1120,20:832,5:4056
step cmd set 0 15 2058 - - 0 0
step ticks 4
step cmd scan 0 20 44
step ticks 8
step cmd set 0 1 28 r55427263 - 0 0
step cmd copy 0 0 5 0
step cmd get 0 15
step cmd set 0 2 1275 - - 0 0
step cmd meta 0 9
step cmd append 0 0 287
step cmd barrier
step cmd mset 0 0:578,0:3006,5:42195
step cmd set 0 7 2422 r171 nx 0 0
step cmd append 0 10 97
step cmd set 0 1 64111 r2666 - 0 0
step cmd set 0 4 11 r4966 xx 0 0
step cmd set 0 13 27 - - 0 0
step cmd append 0 0 69
step cmd expire 0 6 r29732414 1
step cmd rename 0 8 18 0
step cmd del 0 0
step cmd copy 0 1 6 0
step cmd setrange 0 15 473 258
step cmd del 0 10
step cmd set 0 19 205 - - 0 0
step cmd append 0 0 100
step cmd mset 0 1:398,6:13,6:10
step ticks 4
step ticks 6
step cmd rename 0 0 10 0
step cmd get 0 0
step clock 18
step cmd set 0 1 20 r168 - 0 0
step cmd set 0 17 957 - - 0 0
step cmd del 0 2,17,7
step cmd del 0 15,0
step cmd mset 0 9:3434,14:864,14:49858
step cmd set 0 6 1120 r79 - 0 0
step cmd set 0 1 2925 r4867 xx 0 0
step cmd msetnx 0 19:210,9:90,19:550,9:2021
step ticks 4
step cmd get 0 0
step cmd get 0 1
step cmd setrange 0 1 1116 325
step ticks 4
step cmd msetnx 0 20:9,0:19,10:4092
step cmd copy 0 7 12 1
step cmd setrange 0 4 1105 418
step cmd scan 0 13 34
step cmd setrange 0 9 599 244
step cmd set 0 2 1164 r5289333 xx 0 0
step cmd set 0 1 611 r18920522 nx 0 0
step cmd copy 0 20 10 1
step cmd set 0 11 1260 r74585754 nx 0 0
step cmd del 0 1,1,1
step cmd append 0 1 122
step cmd set 0 20 292 - - 0 0
step cmd meta 0 5
step ticks 6
step cmd set 0 16 1902 - nx 0 0
step cmd set 0 0 757 - - 0 0
step cmd append 0 7 131
step cmd mset 0 0:44286,15:13,15:32
step cmd del 0 9
step cmd setrange 0 10 839 510
step cmd setrange 0 1 305 168
step cmd del 0 16,6,11
step cmd del 0 13
step cmd getdel 0 1
step cmd append 0 1 88
step ticks 4
step cmd del 0 0,15
step cmd set 0 1 727 r189 - 1 0
step cmd set 0 0 909 - xx 0 0
step cmd meta 0 17
step ticks 4
step cmd copy 0 1 6 1
step cmd setrange 0 3 137 405
step cmd append 0 0 240
step cmd append 0 0 109
step ticks 4
step cmd msetnx 0 16:2025,11:21
step cmd del 0 1,16,11
step cmd setrange 0 1 247 318
step cmd del 0 13
step cmd get 0 0
step cmd set 0 0 2674 - - 0 0
step cmd set 0 0 1058 past - 0 0
step cmd set 0 16 34124 - - 0 0
step cmd set 0 6 3868 - - 0 0
step ticks 1
step cmd setrange 0 1 34 555
step cmd setrange 0 1 722 119
step cmd get 0 1
step cmd setrange 0 1 984 522
step cmd msetnx 0 17:869,2:103,2:1
step ticks 6
step cmd append 0 12 6
step ticks 1
step cmd expire 0 0 r4116 0
step ticks 8
step ticks 2
step cmd getdel 0 1
step ticks 3
step quiesce
step cmd append 0 1 118
step cmd flush 0
step cmd set 0 0 2344 - nx 0 0
step cmd get 0 19
"#,
    ),
    (
        "get_reports_corrupt_page",
        r#"kv-sim storage case 1
seed 50438
fail ENGINE 26 GET-corrupt
cfg partitions 16
cfg devices 1
cfg ags 57
cfg lba 512
cfg page_mode two-image
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 0
cfg keys 20
cfg groups 3
cfg dbs 2
cfg profile large
cfg steps 359
cfg rounds 3
cfg tick_permille 120
cfg delay_max 0
cfg late_pct 53
cfg skip_pct 0
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 0 4 1470 r3865 - 0 0
step cmd msetnx 1 0:837402,15:305,9:98
step cmd setrange 0 6 94 301
step cmd set 0 0 340034 past - 0 0
step cmd meta 0 1
step ticks 19
step cmd set 0 0 2524 - - 0 0
step cmd mset 1 0:17007,6:808
step cmd getdel 0 0
step cmd set 1 1 60939 - - 0 0
step cmd setrange 1 0 903 284
step cmd get 0 1
step cmd set 0 1 46689 r107 - 0 0
step cmd meta 1 13
step cmd expire 0 13 - 1
step cmd rename 1 0 18 0
step cmd expire 0 18 - 1
step cmd del 0 14
step cmd expire 1 18 - 1
step cmd set 1 5 140487 r66 - 0 0
step cmd msetnx 0 1:12077226,4:1901112,7:384,4:2056
step cmd meta 0 0
step cmd getdel 0 19
step cmd set 0 1 18860 r1855 - 0 1
step cmd set 0 0 8929709 r83 - 0 0
step cmd append 0 0 270
"#,
    ),
    (
        "acked_write_lost_after_power_cut_2",
        r#"kv-sim storage case 1
seed 50114
fail I1 17 power_cut-Power
cfg partitions 16
cfg devices 1
cfg ags 45
cfg lba 512
cfg page_mode two-image
cfg atomic_unit -
cfg cache wb
cfg dsync 1
cfg durability written
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 1
cfg keys 11
cfg groups 5
cfg dbs 1
cfg profile churn
cfg steps 635
cfg rounds 5
cfg tick_permille 40
cfg delay_max 4
cfg late_pct 58
cfg skip_pct 10
cfg per_poll 0
cfg shuffle 0
cfg tick_ms 2
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd get 0 0
step cmd set 0 0 956 r4040 - 0 0
step cmd persist 0 0
step cmd del 0 0,10
step cmd scan 0 6 38
step cmd set 0 0 11 - - 0 0
step cmd msetnx 0 10:827,10:473,10:2042
step cmd msetnx 0 6:19366,1:830,1:1973,1:548
step ticks 4
step cmd set 0 0 2809 r113 - 0 0
step cmd msetnx 0 8:2072,8:55906
step cmd del 0 0,0,10
step cmd set 0 6 1410 r150 - 0 0
step cmd set 0 8 2084 - xx 1 0
step cmd get 0 4
step ticks 5
step cmd flush all
step crash power prefix 11
"#,
    ),
    (
        "acked_write_lost_after_kill",
        r#"kv-sim storage case 1
seed 50037
fail I1 56 kill_-9-Process
cfg partitions 16
cfg devices 2
cfg ags 27
cfg lba 4096
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 0
cfg keys 38
cfg groups 9
cfg dbs 1
cfg profile churn
cfg steps 834
cfg rounds 4
cfg tick_permille 120
cfg delay_max 4
cfg late_pct 2
cfg skip_pct 0
cfg per_poll 1
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd del 0 32
step cmd msetnx 0 2:10,29:31
step cmd setrange 0 21 357 287
step cmd get 0 0
step cmd set 0 19 2137 r91 nx 0 0
step cmd getdel 0 1
step cmd del 0 1
step ticks 3
step cmd get 0 3
step cmd scan 0 6 16
step cmd meta 0 2
step cmd mset 0 1:2655,37:612,37:32
step cmd set 0 0 694 - - 0 0
step ticks 58
step cmd rename 0 14 14 0
step cmd append 0 1 2150
step cmd append 0 0 61
step cmd expire 0 11 - 1
step cmd get 0 26
step cmd set 0 35 53414 r1748 - 0 0
step cmd setrange 0 20 107 368
step cmd set 0 22 1 r27 - 1 1
step cmd set 0 35 766 r50 - 0 0
step cmd set 0 1 768 r83915886 - 0 0
step cmd del 0 1,37
step cmd set 0 1 1287 - - 0 0
step ticks 8
step cmd del 0 0
step ticks 4
step cmd mset 0 1:2981,10:14,10:1487,28:1
step cmd flush all
step cmd rename 0 3 3 0
step cmd set 0 3 817 r3267 - 0 0
step cmd rename 0 0 27 1
step ticks 6
step cmd set 0 6 34710 r41 - 0 0
step cmd set 0 37 1399 r89 - 0 0
step cmd setrange 0 32 758 481
step cmd barrier
step cmd get 0 1
step cmd set 0 3 0 - - 0 0
step cmd set 0 27 1130 - - 1 0
step cmd scan 0 7 31
step cmd del 0 32,5,14
step cmd persist 0 1
step cmd set 0 27 819 - - 0 0
step cmd set 0 21 49264 r3099 nx 0 0
step cmd persist 0 19
step cmd get 0 28
step cmd rename 0 0 36 1
step cmd set 0 22 2007 r132 - 0 0
step cmd set 0 16 2889 - nx 0 0
step cmd set 0 3 652 - - 0 0
step cmd get 0 2
step cmd mset 0 28:27,19:12,1:1426,37:3568
step cmd meta 0 3
step crash kill sectors 0
"#,
    ),
    (
        "multi_key_write_partial_after_kill",
        r#"kv-sim storage case 1
seed 50544
fail I4 165 kill_-9-Process
cfg partitions 16
cfg devices 1
cfg ags 56
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 16384
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 16777216
cfg fill_pct 95
cfg snapshot 1
cfg keys 34
cfg groups 17
cfg dbs 1
cfg profile churn
cfg steps 352
cfg rounds 4
cfg tick_permille 120
cfg delay_max 32
cfg late_pct 16
cfg skip_pct 0
cfg per_poll 0
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd rename 0 0 0 0
step cmd set 0 30 1300 r66600944 - 0 0
step cmd rename 0 21 4 1
step cmd get 0 33
step cmd get 0 0
step cmd set 0 3 1319 - - 0 0
step cmd get 0 2
step cmd rename 0 1 18 0
step cmd set 0 16 398 r2362 - 0 0
step ticks 8
step cmd persist 0 3
step ticks 8
step ticks 46
step cmd scan 0 11 37
step ticks 1
step cmd append 0 24 102
step cmd del 0 22
step cmd mset 0 2:3962,2:2099,19:1041
step cmd getdel 0 0
step cmd getdel 0 1
step cmd setrange 0 0 805 501
step cmd del 0 1,18
step cmd set 0 31 1939 - - 1 0
step cmd get 0 32
step cmd set 0 2 42 r78554577 - 0 0
step cmd set 0 12 24 r879 - 0 0
step cmd set 0 1 1243 r103 - 0 0
step cmd set 0 20 17 r26543906 - 0 0
step cmd set 0 15 2029 past - 0 0
step cmd set 0 14 8 - - 0 0
step cmd set 0 3 1924 - - 0 1
step ticks 59
step cmd get 0 9
step cmd setrange 0 30 1131 557
step cmd setrange 0 4 281 594
step cmd set 0 15 30952 r86 - 0 0
step cmd set 0 18 18 r72547593 xx 0 0
step cmd barrier
step cmd meta 0 7
step cmd set 0 2 1278 - - 1 0
step cmd expire 0 12 r31397142 1
step cmd get 0 5
step cmd append 0 30 284
step ticks 38
step ticks 8
step ticks 52
step cmd setrange 0 30 244 341
step cmd set 0 26 3571 - - 0 0
step cmd rename 0 0 0 1
step cmd setrange 0 22 311 204
step cmd set 0 1 13653 - - 0 0
step cmd get 0 33
step cmd set 0 32 72 r65018922 - 0 0
step cmd set 0 27 1904 - - 0 0
step cmd expire 0 20 - 1
step quiesce
step ticks 8
step cmd copy 0 33 16 1
step cmd append 0 3 295
step cmd expire 0 29 r415 1
step cmd del 0 26,26
step cmd rename 0 22 5 1
step cmd del 0 0,0
step cmd mset 0 3:7,3:2016,3:992,20:1996
step cmd rename 0 31 31 1
step cmd meta 0 25
step cmd msetnx 0 3:25,20:1080,20:740,3:1951
step cmd setrange 0 17 13782 456
step cmd del 0 1
step cmd set 0 1 1921 - - 0 0
step cmd set 0 1 19 - - 0 0
step ticks 9
step ticks 4
step cmd get 0 19
step cmd append 0 0 85204
step cmd append 0 0 156
step cmd append 0 3 120
step cmd get 0 3
step cmd flush all
step cmd set 0 23 2504 r36 - 0 0
step cmd set 0 2 1083 r73590706 - 0 0
step cmd get 0 12
step cmd set 0 2 2030 - nx 0 0
step ticks 4
step ticks 1
step cmd del 0 24,24,7
step ticks 3
step cmd set 0 26 2041 - - 0 0
step crash power bytes 574
step cmd meta 0 16
step cmd msetnx 0 2:18,2:900,19:1090,19:9
step cmd scan 0 0 32
step cmd rename 0 13 30 0
step cmd append 0 3 158
step cmd del 0 3,3,20
step cmd get 0 2
step cmd set 0 2 2037 - - 0 0
step cmd del 0 0,17
step cmd set 0 0 13814 - - 0 0
step cmd set 0 25 950 - - 0 0
step cmd get 0 9
step cmd expire 0 12 - 1
step cmd get 0 2
step cmd setrange 0 12 699 195
step cmd set 0 26 21 - - 0 0
step cmd rename 0 20 3 0
step ticks 5
step ticks 6
step cmd scan 0 8 10
step ticks 6
step cmd append 0 24 36
step cmd set 0 21 1494 past - 0 0
step cmd set 0 1 3 r3676 - 0 0
step cmd get 0 3
step cmd set 0 11 2277 - - 0 0
step cmd set 0 7 1927 - xx 0 0
step cmd set 0 3 223 r189 - 0 0
step ticks 48
step cmd msetnx 0 15:1374,32:18,32:461,15:24
step cmd del 0 26
step cmd msetnx 0 1:1938,18:2045,18:63673,18:968
step cmd del 0 19,2,19
step cmd mset 0 16:5,33:3285,33:1978
step cmd getdel 0 0
step cmd del 0 0
step cmd append 0 1 54
step cmd set 0 9 3047 - - 0 0
step cmd expire 0 17 - 1
step cmd set 0 0 20128 - xx 1 0
step cmd set 0 6 3 - - 0 0
step cmd rename 0 4 21 0
step cmd set 0 28 25 - - 0 0
step cmd expire 0 29 - 1
step cmd copy 0 26 9 1
step cmd get 0 19
step ticks 1
step cmd del 0 10
step cmd set 0 17 2080 - - 0 0
step ticks 1
step cmd append 0 5 4
step ticks 8
step ticks 3
step cmd del 0 0,0
step cmd set 0 0 0 - - 0 0
step cmd msetnx 0 8:16,25:742,25:727
step cmd set 0 5 28 - - 0 0
step cmd del 0 0
step cmd copy 0 30 13 0
step ticks 2
step cmd set 0 3 15 - - 0 0
step cmd append 0 0 170
step cmd append 0 24 88
step cmd set 0 1 25 - - 0 1
step cmd set 0 11 2061 r1739 - 0 0
step cmd del 0 32,32,32
step cmd mset 0 1:56373,18:1211,1:116,1:8316
step cmd set 0 9 437 - nx 0 0
step cmd set 0 18 7 r912 xx 0 0
step cmd expire 0 4 - 0
step ticks 3
step cmd append 0 25 271
step cmd del 0 0,17,17
step ticks 4
step cmd barrier
step ticks 2
step crash kill sectors 4
"#,
    ),
    (
        "two_extent_copies_after_quiesce",
        r#"kv-sim storage case 1
seed 50039
fail I11 28 extra-copies
cfg partitions 32
cfg devices 1
cfg ags 99
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 16777216
cfg fill_pct 95
cfg snapshot 0
cfg keys 30
cfg groups 10
cfg dbs 1
cfg profile large
cfg steps 354
cfg rounds 4
cfg tick_permille 250
cfg delay_max 32
cfg late_pct 56
cfg skip_pct 0
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 0 2 548164 r19 nx 0 0
step cmd set 0 9 5131012 r4175 - 0 0
step cmd copy 0 15 5 0
step cmd set 0 0 6922 r144 - 0 0
step cmd persist 0 3
step ticks 6
step cmd set 0 20 44121 - - 0 0
step cmd set 0 10 1034818 past - 0 0
step cmd expire 0 10 r118 0
step cmd msetnx 0 11:246,11:8144434,1:46836
step ticks 4
step cmd get 0 11
step ticks 4
step cmd set 0 0 41098 - - 0 0
step cmd set 0 0 2090 - - 0 0
step ticks 7
step cmd meta 0 0
step cmd set 0 24 944 - - 0 0
step ticks 1
step ticks 2
step cmd append 0 2 220
step cmd set 0 20 2448200 - - 0 0
step ticks 7
step cmd set 0 0 3238 - - 0 0
step cmd expire 0 2 past 1
step cmd set 0 1 1922 - nx 1 0
step cmd get 0 2
step cmd set 0 0 95189 - - 0 1
"#,
    ),
    (
        "orphans_after_clean_restart_2",
        r#"kv-sim storage case 1
seed 50032
fail I10 155 clean_restart
cfg partitions 16
cfg devices 2
cfg ags 25
cfg lba 512
cfg page_mode two-image
cfg atomic_unit 512
cfg cache wt
cfg dsync 0
cfg durability written
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 1
cfg keys 194
cfg groups 64
cfg dbs 1
cfg profile mixed
cfg steps 1401
cfg rounds 2
cfg tick_permille 450
cfg delay_max 32
cfg late_pct 18
cfg skip_pct 40
cfg per_poll 2
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step ticks 2
step ticks 6
step ticks 53
step ticks 3
step cmd set 0 17 401 - - 0 0
step ticks 2
step cmd meta 0 8
step cmd mset 0 10:11,10:2003,138:17,10:268764
step cmd set 0 35 1941 past - 0 0
step cmd msetnx 0 7:765,135:759
step ticks 28
step ticks 7
step cmd expire 0 105 r178 0
step ticks 8
step cmd del 0 138,138,138
step ticks 7
step ticks 5
step ticks 4
step ticks 6
step cmd persist 0 20
step cmd set 0 185 1105 - - 0 0
step cmd set 0 96 30 - - 0 0
step cmd getdel 0 126
step ticks 3
step ticks 3
step ticks 8
step cmd get 0 135
step ticks 4
step cmd mset 0 3:1178,3:181049,131:2031
step ticks 8
step ticks 4
step cmd expire 0 19 r1852 0
step cmd setrange 0 48 1086 313
step ticks 2
step cmd get 0 2
step cmd expire 0 114 - 1
step ticks 43
step ticks 6
step ticks 14
step cmd set 0 1 1178 - - 0 0
step ticks 1
step ticks 1
step cmd msetnx 0 14:1991,78:2043
step cmd persist 0 19
step ticks 4
step cmd msetnx 0 153:761,153:17
step ticks 8
step cmd copy 0 17 145 0
step cmd persist 0 104
step cmd set 0 14 2485 - - 0 0
step cmd set 0 72 2004 r91 - 0 0
step cmd getdel 0 143
step cmd mset 0 174:1276,110:4900917
step ticks 8
step ticks 53
step cmd get 0 9
step ticks 8
step ticks 60
step cmd set 0 6 115 - - 0 0
step ticks 5
step cmd set 0 1 215 - - 0 0
step cmd scan 0 11 28
step ticks 5
step cmd set 0 15 61248 - - 0 0
step ticks 1
step cmd mset 0 105:45988,169:23
step cmd get 0 147
step ticks 3
step cmd get 0 14
step ticks 8
step cmd persist 0 151
step cmd getdel 0 180
step ticks 5
step cmd getdel 0 109
step ticks 7
step ticks 1
step ticks 7
step cmd append 0 23 21
step ticks 3
step ticks 34
step ticks 2
step cmd rename 0 6 134 0
step cmd set 0 9 1408 - - 0 0
step cmd set 0 23 13 - xx 0 0
step cmd copy 0 142 78 1
step cmd set 0 11 371 r136 - 0 0
step cmd barrier
step ticks 5
step cmd set 0 159 1080 - nx 0 0
step ticks 1
step cmd expire 0 24 r111 1
step ticks 54
step cmd getdel 0 7
step cmd get 0 13
step ticks 26
step ticks 7
step ticks 2
step ticks 6
step ticks 31
step ticks 4
step cmd set 0 107 2971 r5791982 - 0 0
step ticks 2
step ticks 3
step ticks 5
step cmd set 0 26 8 - - 0 0
step ticks 8
step cmd del 0 17,145
step cmd set 0 125 11460932 r30 - 0 0
step cmd append 0 17 2050
step ticks 8
step ticks 56
step cmd get 0 74
step cmd get 0 96
step ticks 6
step ticks 4
step cmd set 0 22 20 - - 0 0
step ticks 4
step cmd set 0 21 144 r3824023 - 0 1
step clock 35
step cmd persist 0 10
step ticks 21
step ticks 2
step cmd expire 0 72 - 1
step cmd setrange 0 67 64033 346
step ticks 5
step cmd msetnx 0 0:27,128:1327,0:1984,128:997
step cmd rename 0 15 15 0
step cmd set 0 139 3301 past - 0 0
step ticks 44
step ticks 54
step cmd expire 0 170 - 0
step cmd get 0 8
step cmd set 0 8 1351 - - 0 1
step ticks 1
step cmd set 0 22 2978 r1306 - 0 0
step ticks 43
step clock 6
step ticks 5
step cmd append 0 162 299
step cmd get 0 91
step cmd setrange 0 8 50 513
step cmd mset 0 99:3405,35:654,35:806,163:113
step cmd get 0 26
step ticks 7
step ticks 3
step ticks 6
step cmd setrange 0 20 84 126
step cmd set 0 113 424 - - 0 0
step quiesce
step cmd mset 0 105:2961,105:942
step cmd expire 0 164 - 1
step ticks 27
step cmd get 0 20
step cmd get 0 57
step ticks 3
step clean
"#,
    ),
    (
        "expired_copy_left_beside_new_extent",
        r#"kv-sim storage case 1
seed 20002
fail I11 5 expired-leftover
cfg partitions 16
cfg devices 1
cfg ags 46
cfg lba 4096
cfg page_mode two-image
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 16777216
cfg fill_pct 95
cfg snapshot 1
cfg keys 31
cfg groups 31
cfg dbs 1
cfg profile large
cfg steps 438
cfg rounds 1
cfg tick_permille 250
cfg delay_max 0
cfg late_pct 25
cfg skip_pct 0
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 1
cfg max_len 8000000
cfg unit_cap 4096
step cmd set 0 29 10 r93 - 0 0
step ticks 3
step ticks 1
step clock 150
step cmd msetnx 0 29:476840
"#,
    ),
    (
        "persist_after_expire_misses_key",
        r#"kv-sim storage case 1
seed 1169
fail RESULT 4 PERSIST-result
cfg partitions 16
cfg devices 1
cfg ags 49
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 67108864
cfg fill_pct 95
cfg snapshot 0
cfg keys 90
cfg groups 90
cfg dbs 1
cfg profile mixed
cfg steps 1299
cfg rounds 2
cfg tick_permille 40
cfg delay_max 32
cfg late_pct 29
cfg skip_pct 40
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 1
cfg max_len 8000000
cfg unit_cap 4096
step cmd msetnx 0 3:12,3:88473,3:2035
step ticks 5
step cmd expire 0 3 r11852688 1
step cmd persist 0 3
"#,
    ),
    (
        "get_after_expire_returns_old_value",
        r#"kv-sim storage case 1
seed 1032
fail RESULT 4 GET-result
cfg partitions 32
cfg devices 1
cfg ags 71
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability written
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 1
cfg keys 36
cfg groups 7
cfg dbs 1
cfg profile mixed
cfg steps 671
cfg rounds 3
cfg tick_permille 40
cfg delay_max 32
cfg late_pct 53
cfg skip_pct 0
cfg per_poll 8
cfg shuffle 0
cfg tick_ms 2
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 1
cfg max_len 8000000
cfg unit_cap 4096
step cmd mset 0 28:0,14:593,14:2073
step quiesce
step cmd expire 0 14 r62 1
step cmd get 0 14
"#,
    ),
    (
        "chunkmap_persist_after_expire_reports_corrupt",
        r#"kv-sim storage case 1
seed 50259
fail ENGINE 3 PERSIST-corrupt
cfg partitions 16
cfg devices 1
cfg ags 60
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 0
cfg keys 23
cfg groups 4
cfg dbs 1
cfg profile large
cfg steps 489
cfg rounds 1
cfg tick_permille 40
cfg delay_max 4
cfg late_pct 10
cfg skip_pct 0
cfg per_poll 8
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 0 21 11730655 - - 0 0
step cmd expire 0 21 r71819649 0
step cmd persist 0 21
"#,
    ),
    (
        "del_after_reset_of_expired_key_miscounts",
        r#"kv-sim storage case 1
seed 51620
fail RESULT 5 DEL-result
cfg partitions 16
cfg devices 1
cfg ags 49
cfg lba 4096
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 16777216
cfg fill_pct 95
cfg snapshot 1
cfg keys 50
cfg groups 12
cfg dbs 1
cfg profile churn
cfg steps 964
cfg rounds 5
cfg tick_permille 250
cfg delay_max 32
cfg late_pct 21
cfg skip_pct 0
cfg per_poll 0
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 0 3 2260 r5 - 0 0
step ticks 7
step cmd set 0 3 2025 - - 0 0
step clock 55
step cmd del 0 27,27,3
"#,
    ),
    (
        "large_mset_deferred_page_set_hang",
        r#"kv-sim storage case 1
seed 50692
fail HANG 11 SET_ticket-Executed-needs-Durable
cfg partitions 16
cfg devices 1
cfg ags 69
cfg lba 512
cfg page_mode two-image
cfg atomic_unit -
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 1
cfg keys 29
cfg groups 7
cfg dbs 1
cfg profile large
cfg steps 300
cfg rounds 1
cfg tick_permille 120
cfg delay_max 200
cfg late_pct 43
cfg skip_pct 10
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 2
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd append 0 5 114
step cmd mset 0 2:10246221,23:778,16:9216,23:1145
step cmd set 0 22 19992 - - 0 0
step cmd msetnx 0 8:4171533,1:3901,1:55032
step cmd del 0 1,22,22
step cmd append 0 1 1
step ticks 56
step cmd set 0 1 192608 - - 0 0
step cmd append 0 12 128
step ticks 6
step cmd set 0 1 958 - - 0 0
"#,
    ),
    (
        "rename_set_behind_deferred_page_hang",
        r#"kv-sim storage case 1
seed 50746
fail HANG 79 SET_ticket-Executed-needs-Durable
cfg partitions 128
cfg devices 1
cfg ags 281
cfg lba 4096
cfg page_mode two-image
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 16777216
cfg fill_pct 95
cfg snapshot 1
cfg keys 34
cfg groups 5
cfg dbs 2
cfg profile churn
cfg steps 330
cfg rounds 1
cfg tick_permille 40
cfg delay_max 4
cfg late_pct 31
cfg skip_pct 0
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step ticks 5
step cmd set 1 3 1194 - - 0 0
step cmd mset 0 12:3,27:27163
step cmd rename 1 27 27 0
step cmd set 0 33 1936 - nx 0 0
step cmd mset 0 0:664,30:18,15:1959
step clock 16
step cmd append 0 4 192
step cmd set 0 0 1918 r29482931 - 0 0
step cmd set 1 0 1911 - - 0 0
step cmd expire 0 1 - 1
step cmd flush 0
step cmd set 0 2 2061 - - 0 0
step cmd set 1 12 8 r47002884 - 0 0
step cmd set 0 2 1944 - - 0 0
step cmd set 0 19 9 - - 0 0
step cmd set 0 18 2072 - - 1 0
step cmd append 0 7 18
step cmd del 1 31,31
step cmd set 0 13 1377 r4037 - 0 0
step cmd mset 0 0:12,20:2027,10:2048,15:451
step cmd expire 1 2 - 0
step cmd append 0 3 1913
step cmd mset 0 3:24,13:1916
step cmd copy 1 2 27 1
step cmd set 0 0 1194 - nx 0 0
step cmd setrange 1 31 437 460
step cmd setrange 1 32 434 193
step cmd set 0 33 2092 - - 0 0
step cmd del 0 2,7
step cmd set 0 15 1148 - - 0 0
step cmd set 0 0 16 - - 0 0
step cmd get 1 3
step cmd get 0 0
step cmd set 0 3 48874 - - 0 0
step cmd set 0 24 2007 - - 0 0
step cmd set 1 2 22 r78251384 nx 0 0
step cmd setrange 0 17 8573 327
step cmd get 1 3
step cmd msetnx 0 29:353,19:20
step cmd del 0 3,8,18
step cmd expire 1 29 r3579 0
step cmd set 0 14 60598 r3416 - 0 0
step cmd del 0 3
step cmd set 0 21 1393 r3450 - 0 0
step cmd del 0 26,16,16
step cmd set 0 1 53269 - xx 0 0
step cmd expire 0 17 - 0
step cmd persist 0 0
step cmd setrange 0 33 84 599
step ticks 6
step cmd set 0 15 21 r2381 - 0 0
step cmd append 0 9 260
step ticks 6
step cmd getdel 0 3
step cmd get 0 31
step cmd set 0 3 398 - - 0 0
step cmd get 0 1
step cmd expire 0 17 r1274 0
step cmd set 1 3 1912 - - 0 0
step cmd scan 0 38 2
step cmd set 0 17 1993 - - 0 0
step cmd set 0 1 386 r3051 - 0 0
step ticks 1
step cmd set 1 1 31607 - nx 0 0
step cmd del 0 1
step cmd copy 0 11 6 1
step cmd set 0 3 1928 r4603 - 0 0
step cmd rename 0 3 23 0
step cmd del 0 3,13
step cmd get 1 0
step cmd del 0 2,32
step cmd del 0 0,10,20
step cmd copy 1 0 30 1
step cmd set 0 22 577 - - 0 0
step cmd del 0 1,21
step cmd set 1 0 541 - xx 0 0
step cmd set 0 1 4085 r1777 - 0 0
step ticks 5
"#,
    ),
    (
        "msetnx_barrier_hang_async",
        r#"kv-sim storage case 1
seed 51101
fail HANG 94 BARRIER_ticket-Executed-needs-Durable
cfg partitions 16
cfg devices 1
cfg ags 41
cfg lba 4096
cfg page_mode two-image
cfg atomic_unit -
cfg cache wb
cfg dsync 0
cfg durability async
cfg cache_bytes 16777216
cfg fill_pct 95
cfg snapshot 1
cfg keys 38
cfg groups 6
cfg dbs 1
cfg profile mixed
cfg steps 1398
cfg rounds 5
cfg tick_permille 250
cfg delay_max 32
cfg late_pct 18
cfg skip_pct 0
cfg per_poll 2
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd get 0 2
step ticks 8
step cmd del 0 11,5
step ticks 54
step cmd msetnx 0 24:399,36:1460,30:1071,12:5
step cmd msetnx 0 3:4,33:1944,15:3257,33:58347
step cmd msetnx 0 2:309098,14:2,8:20
step cmd get 0 1
step cmd msetnx 0 20:524,8:873,2:30,20:3460
step cmd flush 0
step cmd msetnx 0 22:39850,22:846,4:13
step ticks 33
step cmd set 0 32 783 r72 - 1 0
step cmd set 0 12 13410 - - 0 0
step ticks 27
step ticks 7
step cmd set 0 0 95173 - - 0 0
step cmd scan 0 3 44
step cmd setrange 0 19 840 340
step cmd expire 0 1 - 0
step cmd set 0 9 19 - - 0 0
step ticks 3
step cmd append 0 0 153
step cmd mset 0 1:985,31:12354,1:30
step cmd get 0 25
step quiesce
step cmd msetnx 0 6:48216,6:1
step cmd rename 0 34 16 1
step clock 33
step cmd set 0 23 1056 - xx 0 0
step cmd set 0 9 712611 - - 0 0
step ticks 8
step cmd get 0 0
step cmd expire 0 1 - 1
step cmd setrange 0 1 158 3
step cmd append 0 34 2
step cmd scan 0 7 16
step cmd append 0 1 168
step cmd rename 0 31 1 0
step cmd set 0 1 2083 r4097 - 0 0
step cmd append 0 8 187
step ticks 1
step cmd meta 0 1
step cmd mset 0 17:389,11:3006,23:65134,17:120
step cmd set 0 7 2072 r158 - 0 0
step cmd barrier
step cmd get 0 12
step cmd get 0 8
step cmd get 0 8
step cmd copy 0 1 7 1
step cmd set 0 3 814 - - 0 0
step cmd rename 0 1 7 0
step cmd meta 0 21
step cmd set 0 25 577 - - 0 1
step cmd scan 0 14 24
step cmd set 0 3 29525 r80782145 - 0 0
step cmd set 0 12 285 - - 0 0
step cmd mset 0 4:3502,34:1980,10:1,4:690
step ticks 8
step ticks 3
step cmd expire 0 31 - 1
step cmd set 0 14 1956 - nx 0 0
step cmd meta 0 5
step clock 8
step cmd set 0 2 2958 r29 - 0 0
step cmd meta 0 0
step cmd copy 0 0 24 1
step cmd get 0 3
step cmd del 0 30,36,12
step cmd getdel 0 1
step cmd set 0 1 1187 r55355556 nx 0 0
step cmd persist 0 26
step cmd set 0 1 841 - - 0 0
step cmd meta 0 20
step cmd set 0 0 992 r4619 - 0 0
step cmd set 0 1 2119 - - 0 1
step cmd get 0 0
step cmd expire 0 34 r75854760 0
step cmd set 0 18 784 r168 - 0 0
step cmd set 0 12 1146 - - 0 0
step cmd barrier
step cmd mset 0 29:672225,17:2804
step cmd copy 0 2 26 1
step cmd mset 0 1:88,7:694
step cmd rename 0 2 14 0
step cmd expire 0 20 - 1
step cmd del 0 3
step ticks 2
step cmd msetnx 0 1:706,37:4025
step ticks 2
step cmd rename 0 25 37 0
step cmd setrange 0 2 69217 256
step ticks 2
step cmd set 0 24 813 r479 - 0 0
"#,
    ),
    (
        "inplace_rewrite_cancelled_leaves_old_extent",
        r#"kv-sim storage case 1
seed 100047
fail I11 9 extra-copies
cfg partitions 16
cfg devices 2
cfg ags 28
cfg lba 4096
cfg page_mode atomic
cfg atomic_unit 8192
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 1
cfg keys 17
cfg groups 3
cfg dbs 1
cfg profile churn
cfg steps 920
cfg rounds 5
cfg tick_permille 120
cfg delay_max 32
cfg late_pct 26
cfg skip_pct 10
cfg per_poll 2
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 0 0 2033 r16200372 - 0 0
step ticks 1
step ticks 1
step cmd setrange 0 0 1184 21
step cmd del 0 0
step cmd set 0 0 3956 r143 - 0 0
step cmd expire 0 0 - 1
step cmd set 0 0 16 - - 0 0
step cmd set 0 1 2185 - - 0 0
"#,
    ),
    (
        "get_error_after_failed_write_is_not_absence",
        r#"kv-sim storage case 1
seed 327
fail I2 172 ioerr_scan-end_of_run
cfg partitions 16
cfg devices 1
cfg ags 51
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 16384
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 0
cfg keys 92
cfg groups 46
cfg dbs 1
cfg profile mixed
cfg steps 1010
cfg rounds 2
cfg tick_permille 450
cfg delay_max 0
cfg late_pct 30
cfg skip_pct 10
cfg per_poll 8
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 20000
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd expire 0 84 r172 0
step ticks 45
step ticks 3
step ticks 7
step cmd msetnx 0 3:25,3:60763,3:1163
step ticks 6
step cmd append 0 6 216
step cmd set 0 9 1469 r3112 - 0 0
step ticks 4
step ticks 5
step cmd set 0 8 2714 - - 0 0
step cmd expire 0 33 - 0
step ticks 1
step cmd set 0 2 254 - - 0 0
step quiesce
step cmd set 0 50 1341 - - 0 0
step cmd set 0 4 314 r3106 - 0 0
step cmd expire 0 8 - 1
step cmd msetnx 0 24:19,70:683
step ticks 6
step ticks 1
step cmd set 0 5 1092 - xx 0 0
step cmd get 0 83
step cmd set 0 7 795839 - - 0 0
step ticks 29
step ticks 5
step ticks 52
step cmd append 0 91 140
step ticks 1
step ticks 8
step cmd meta 0 58
step cmd set 0 31 888 - nx 0 0
step ticks 8
step ticks 1
step cmd setrange 0 10 457 160
step cmd set 0 21 39578 - - 0 0
step ticks 2
step cmd set 0 10 733 - - 0 0
step ticks 30
step cmd set 0 10 2139 - - 0 0
step cmd get 0 15
step cmd expire 0 20 - 0
step ticks 6
step cmd get 0 3
step cmd setrange 0 36 412 586
step cmd set 0 7 59 r3165 - 0 0
step ticks 8
step cmd setrange 0 54 808 536
step quiesce
step ticks 2
step ticks 1
step ticks 3
step ticks 8
step ticks 56
step cmd set 0 7 32 r5023067 - 0 0
step cmd set 0 69 14 - - 1 0
step cmd msetnx 0 17:357,17:1320,17:11701,63:10611
step cmd append 0 9 224
step ticks 5
step cmd set 0 43 1 - - 0 0
step cmd expire 0 9 - 0
step cmd setrange 0 82 238 588
step ticks 1
step cmd expire 0 13 - 1
step ticks 4
step ticks 4
step cmd expire 0 9 - 0
step cmd setrange 0 5 529 537
step cmd setrange 0 58 42886 467
step ticks 1
step ticks 1
step cmd mset 0 45:533,45:11152903,45:1
step cmd append 0 15 60
step cmd set 0 9 1961 r30222269 - 0 0
step ticks 5
step ticks 3
step ticks 14
step ticks 2
step cmd persist 0 1
step ticks 6
step cmd copy 0 33 33 1
step cmd get 0 31
step cmd msetnx 0 81:105,35:29,35:1049
step ticks 1
step cmd append 0 75 181
step cmd meta 0 4
step ticks 7
step ticks 3
step cmd set 0 13 300 - - 0 0
step ticks 1
step ticks 4
step ticks 1
step cmd set 0 15 21 past - 0 0
step cmd del 0 22,22
step cmd set 0 9 7401 - - 0 0
step cmd del 0 2,2
step ticks 6
step ticks 52
step cmd del 0 35,35
step cmd expire 0 10 - 0
step ticks 2
step ticks 2
step ticks 7
step cmd setrange 0 35 769 76
step ticks 5
step cmd rename 0 1 47 1
step cmd set 0 74 1359 - nx 0 0
step cmd mset 0 7:158,7:3
step cmd rename 0 57 11 0
step ticks 27
step ticks 58
step cmd expire 0 49 r43068580 0
step cmd setrange 0 9 675 27
step ticks 7
step ticks 57
step ticks 6
step cmd set 0 6 1142 - - 0 0
step cmd copy 0 15 61 0
step ticks 56
step cmd getdel 0 75
step ticks 2
step cmd del 0 26,26,26
step ticks 6
step ticks 6
step cmd get 0 29
step cmd set 0 9 664 r43902291 - 0 0
step cmd set 0 49 100 r146 - 1 0
step ticks 7
step ticks 2
step ticks 3
step cmd persist 0 8
step ticks 51
step cmd set 0 30 1985 r13 - 0 0
step ticks 6
step ticks 5
step ticks 2
step cmd set 0 44 1973 - - 1 0
step ticks 8
step cmd set 0 7 1958 - - 0 0
step cmd set 0 89 15 r182 - 0 0
step cmd barrier
step ticks 2
step ticks 2
step cmd scan 0 7 29
step ticks 2
step cmd append 0 70 295
step cmd set 0 4 1411 - - 0 0
step cmd get 0 86
step ticks 7
step ticks 48
step cmd del 0 41,41
step ticks 17
step ticks 7
step ticks 32
step ticks 7
step ticks 3
step cmd get 0 32
step ticks 3
step ticks 4
step ticks 9
step cmd set 0 53 2044 r97 - 0 0
step ticks 3
step cmd set 0 4 829 - - 0 0
step cmd setrange 0 63 1035 600
step cmd msetnx 0 47:41164,1:31379
step cmd append 0 50 146
step cmd set 0 72 456 - xx 0 0
step cmd del 0 15,15
step ticks 57
step ticks 2
step cmd setrange 0 3 1050 40
step cmd setrange 0 76 8389101 277
"#,
    ),
    (
        "run_tail_of_dead_extent_hides_a_live_page",
        r#"kv-sim storage case 1
seed 346
fail I11 17 copy-missing
cfg partitions 16
cfg devices 1
cfg ags 48
cfg lba 4096
cfg page_mode two-image
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability written
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 0
cfg keys 38
cfg groups 6
cfg dbs 3
cfg profile large
cfg steps 324
cfg rounds 1
cfg tick_permille 450
cfg delay_max 32
cfg late_pct 24
cfg skip_pct 0
cfg per_poll 2
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd mset 0 35:72089
step cmd msetnx 0 1:934,31:2051,25:767
step cmd set 0 5 15 r56 - 0 0
step ticks 2
step cmd set 0 14 3626 - - 0 0
step cmd set 0 32 10 - - 0 0
step cmd expire 0 1 - 1
step ticks 4
step cmd set 2 0 4997807 - - 1 0
step cmd setrange 0 31 669 147
step ticks 1
step cmd setrange 0 4 1133 451
step ticks 6
step cmd set 2 3 10 r8147346 - 0 1
step cmd set 0 26 14 past - 0 0
step cmd flush all
step cmd setrange 0 31 122 493
"#,
    ),
    (
        "stale_chunkmap_frees_a_live_extent_run",
        r#"kv-sim storage case 1
seed 1054
fail ENGINE 11 GET-corrupt
cfg partitions 16
cfg devices 1
cfg ags 47
cfg lba 512
cfg page_mode two-image
cfg atomic_unit 512
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 16777216
cfg fill_pct 95
cfg snapshot 1
cfg keys 32
cfg groups 32
cfg dbs 1
cfg profile large
cfg steps 253
cfg rounds 5
cfg tick_permille 120
cfg delay_max 0
cfg late_pct 32
cfg skip_pct 0
cfg per_poll 2
cfg shuffle 1
cfg tick_ms 2
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 0 4 2746 - - 0 0
step cmd set 0 23 11012338 r2 - 1 0
step ticks 7
step crash kill none 0
step cmd set 0 3 2608 - - 0 0
step ticks 4
step cmd setrange 0 1 68288 81
step cmd set 0 23 3086 r3269 - 0 0
step crash power none 4
step cmd mset 0 1:60038
step cmd mset 0 1:305897
"#,
    ),
    (
        "failed_chunkmap_write_read_reports_io_error",
        r#"kv-sim storage case 1
seed 565
fail I1 22 ioerr_end_of_run-Process
cfg partitions 16
cfg devices 1
cfg ags 59
cfg lba 4096
cfg page_mode two-image
cfg atomic_unit 4096
cfg cache wb
cfg dsync 0
cfg durability async
cfg cache_bytes 16777216
cfg fill_pct 95
cfg snapshot 0
cfg keys 36
cfg groups 9
cfg dbs 1
cfg profile large
cfg steps 495
cfg rounds 4
cfg tick_permille 120
cfg delay_max 4
cfg late_pct 17
cfg skip_pct 10
cfg per_poll 8
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 20000
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 0 17 9 - - 0 0
step ticks 6
step ticks 3
step cmd set 0 1 2031 r1085 - 0 0
step cmd set 0 0 3027 r163 - 0 0
step cmd msetnx 0 13:77997
step ticks 7
step cmd mset 0 3:41507,12:59380
step cmd del 0 0
step cmd persist 0 1
step cmd mset 0 1:308,1:9402
step cmd rename 0 1 1 0
step ticks 1
step ticks 8
step cmd set 0 2 3366 - - 0 0
step cmd set 0 33 31 r3414 - 0 0
step cmd mset 0 3:2024,30:1110,12:8516445
step cmd mset 0 3:3588,21:492,12:2053,21:9079215
step cmd persist 0 1
step cmd persist 0 3
step cmd expire 0 12 - 1
step cmd setrange 0 34 778 573
"#,
    ),
    (
        "meta_writes_after_a_failed_cycle_tear_both_copies",
        r#"kv-sim storage case 1
seed 1286
fail RECOVER 3 ioerr_open
cfg partitions 128
cfg devices 1
cfg ags 280
cfg lba 4096
cfg page_mode atomic
cfg atomic_unit 8192
cfg cache wb
cfg dsync 0
cfg durability written
cfg cache_bytes 16777216
cfg fill_pct 95
cfg snapshot 0
cfg keys 46
cfg groups 46
cfg dbs 1
cfg profile churn
cfg steps 973
cfg rounds 2
cfg tick_permille 120
cfg delay_max 0
cfg late_pct 41
cfg skip_pct 0
cfg per_poll 8
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 20000
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd mset 0 0:15452
step cmd flush 0
step ticks 2
step crash power bytes 0
"#,
    ),
    (
        "deleted_key_newest_copy_zeroed_before_older",
        r#"kv-sim storage case 1
seed 2961
fail I1 133 kill_-9-Process
cfg partitions 16
cfg devices 1
cfg ags 38
cfg lba 512
cfg page_mode two-image
cfg atomic_unit 512
cfg cache wb
cfg dsync 0
cfg durability async
cfg cache_bytes 67108864
cfg fill_pct 67
cfg snapshot 0
cfg keys 36
cfg groups 36
cfg dbs 3
cfg profile full
cfg steps 694
cfg rounds 3
cfg tick_permille 40
cfg delay_max 32
cfg late_pct 0
cfg skip_pct 10
cfg per_poll 2
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 1 1 698 - - 0 0
step cmd getdel 1 15
step cmd set 0 2 1289 - - 0 1
step cmd set 0 2 1784434 r38399350 - 0 0
step cmd rename 0 10 10 0
step cmd set 2 2 928524 r190 - 0 0
step cmd msetnx 0 10:60163,10:836316,10:63254
step cmd del 0 0
step cmd set 0 1 890950 r108 - 0 0
step quiesce
step cmd msetnx 0 2:60601,2:1576190
step cmd get 0 18
step cmd set 0 2 532 r4259 nx 0 0
step clock 38
step cmd meta 0 3
step cmd set 0 34 270926 - nx 1 0
step cmd mset 0 5:16792,5:4757,5:998724
step cmd del 2 3
step cmd set 2 6 8545 - - 1 0
step cmd set 0 3 899755 - - 0 0
step cmd set 0 1 665 - nx 0 0
step cmd mset 0 3:668477,3:383702
step cmd set 0 14 251089 - - 0 0
step cmd set 1 25 1458 - - 0 0
step cmd set 0 3 9295 r58863488 - 0 0
step cmd set 0 13 488226 - - 0 0
step cmd set 2 2 626726 - - 0 0
step ticks 6
step cmd del 0 8,8,8
step cmd set 0 17 43959 r36372600 - 0 1
step cmd set 2 9 877 - nx 0 0
step cmd set 0 7 44485 r30042248 nx 0 0
step ticks 7
step cmd set 2 33 6116490 r1150 - 0 0
step cmd set 2 1 971356 - - 1 0
step cmd set 2 3 1918001 - - 0 0
step cmd setrange 0 14 10911 25
step cmd mset 0 12:519876,12:660295,12:824921,12:280476
step cmd del 0 1
step cmd get 0 0
step cmd set 2 2 1476116 r23658041 nx 0 0
step cmd del 2 1
step cmd set 1 2 488963 - - 0 0
step cmd set 1 2 61893 r2428 - 0 0
step cmd set 0 1 432999 - - 0 0
step cmd rename 2 34 34 0
step cmd set 0 0 571830 - - 1 0
step cmd del 0 3,3
step cmd mset 0 19:693,19:208807
step cmd mset 0 0:966444,0:19701,0:938439
step cmd set 1 0 22526 r1934 - 0 0
step cmd set 0 14 5613 - - 0 0
step cmd set 1 0 16378 r785 - 0 0
step cmd msetnx 2 25:726792,25:7515253
step cmd append 0 9 72168
step cmd del 2 0
step cmd del 0 17,17
step cmd setrange 1 35 122 526
step cmd set 2 4 379904 - nx 0 0
step cmd meta 0 1
step cmd meta 1 16
step cmd set 0 14 5890028 - - 0 0
step cmd append 0 3 2
step cmd getdel 0 2
step cmd set 0 2 1276471 r16503204 - 0 0
step cmd set 0 2 599565 - - 0 0
step cmd scan 2 2 34
step cmd setrange 1 4 344 448
step cmd del 0 30,30
step cmd set 0 2 103820 - - 0 0
step cmd set 1 2 1154 - - 0 0
step cmd append 0 3 75
step cmd persist 0 1
step cmd set 2 32 487838 - - 1 0
step cmd del 0 30,30,30
step cmd set 1 4 34940 r80942684 nx 0 0
step cmd meta 2 1
step cmd getdel 0 34
step cmd set 0 3 1086 r21847721 - 0 0
step cmd del 0 0,0,0
step cmd mset 2 0:476653,0:234042,0:671178,0:14863
step cmd set 1 16 643 past - 0 0
step cmd msetnx 0 14:13359,14:574,14:57048
step cmd msetnx 0 23:7626095,23:23575
step cmd del 2 11,11,11
step cmd set 0 30 803171 - - 0 0
step cmd set 0 15 428 - - 0 0
step cmd set 0 4 32367 - - 0 0
step cmd set 0 25 33956 - - 0 0
step cmd set 0 34 967532 r46194334 - 0 0
step cmd set 0 27 110898 r40584661 - 0 0
step cmd del 0 0
step quiesce
step cmd set 0 22 160792 - - 0 0
step cmd del 0 3,3
step cmd set 0 18 5681 - - 0 0
step quiesce
step ticks 3
step cmd del 2 2,2,2
step cmd del 0 27
step cmd msetnx 2 24:943334,24:468653,24:32230,24:37784
step cmd set 0 14 43469 - - 0 0
step cmd append 0 11 39
step ticks 59
step cmd get 2 18
step clock 1509
step cmd set 0 3 1495 r206 - 0 1
step cmd get 0 1
step cmd set 0 1 33730 r2237 - 0 0
step cmd set 2 32 923 r144 - 0 0
step cmd append 1 1 211
step quiesce
step cmd set 0 20 41 r31621343 - 0 0
step cmd del 0 17
step cmd append 0 19 46558
step cmd set 0 31 676669 r3167 - 0 0
step cmd append 0 2 278
step cmd set 0 3 849 r61980478 - 1 0
step cmd getdel 2 33
step cmd set 0 0 29554 r43919396 - 0 0
step cmd append 0 3 190
step cmd set 1 1 198 r74243069 - 0 0
step cmd expire 0 2 - 1
step cmd set 0 1 20444 r59360068 - 0 0
step cmd mset 0 3:699347,3:63258,3:4121301
step cmd set 0 29 66003 r163 - 1 0
step cmd set 2 33 817801 past xx 0 0
step ticks 5
step cmd del 0 1
step cmd del 0 3,3,3
step cmd del 0 2,2
step cmd set 0 8 574 - nx 0 0
step cmd set 1 31 8278864 - - 0 0
step crash kill sectors 7
"#,
    ),
    (
        "expired_unwritten_version_acked_before_older_copies_went",
        r#"kv-sim storage case 1
seed 6985
fail I1 7 kill_-9-Process
cfg partitions 16
cfg devices 2
cfg ags 22
cfg lba 512
cfg page_mode two-image
cfg atomic_unit -
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 1
cfg keys 49
cfg groups 49
cfg dbs 2
cfg profile churn
cfg steps 764
cfg rounds 4
cfg tick_permille 120
cfg delay_max 200
cfg late_pct 50
cfg skip_pct 0
cfg per_poll 1
cfg shuffle 0
cfg tick_ms 5
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 0 5 13 - nx 0 0
step cmd set 0 5 2914 - - 0 0
step cmd del 0 5
step ticks 5
step cmd set 0 5 2082 r10 - 0 0
step ticks 5
step cmd get 0 5
step crash kill sectors 1
"#,
    ),
    (
        "expired_unwritten_extent_power_cut",
        r#"kv-sim storage case 1
seed 8885
fail I1 17 power_cut-Power
cfg partitions 16
cfg devices 1
cfg ags 41
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 4096
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 0
cfg keys 15
cfg groups 15
cfg dbs 2
cfg profile large
cfg steps 311
cfg rounds 4
cfg tick_permille 120
cfg delay_max 200
cfg late_pct 38
cfg skip_pct 0
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 1 11 3058 - - 1 0
step cmd mset 1 7:18926
step cmd del 1 11
step ticks 1
step cmd set 0 0 2162 r2150 - 0 0
step cmd msetnx 0 9:11898958,9:785654
step cmd set 0 14 9 r28250832 - 0 0
step cmd append 1 0 10
step cmd setrange 1 4 24 323
step cmd mset 0 9:3290
step cmd set 0 6 3891 r105 - 1 0
step ticks 3
step cmd set 0 9 3100 r1 - 0 0
step cmd set 0 14 3787 - - 0 0
step cmd expire 0 9 r4774 0
step cmd append 0 7 15
step ticks 4
step crash power sectors 0
"#,
    ),
    (
        "relocated_record_resurrects_flushed_key",
        r#"kv-sim storage case 1
seed 1297
fail RESULT 127 APPEND-result
cfg partitions 16
cfg devices 1
cfg ags 38
cfg lba 512
cfg page_mode atomic
cfg atomic_unit 8192
cfg cache wb
cfg dsync 0
cfg durability written
cfg cache_bytes 1048576
cfg fill_pct 95
cfg snapshot 0
cfg keys 127
cfg groups 25
cfg dbs 2
cfg profile mixed
cfg steps 1148
cfg rounds 2
cfg tick_permille 40
cfg delay_max 32
cfg late_pct 12
cfg skip_pct 0
cfg per_poll 1
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd get 0 91
step cmd set 0 73 1478 r148 - 1 1
step cmd append 1 105 156
step cmd set 0 39 24 - - 0 0
step cmd setrange 0 28 313 242
step cmd setrange 0 0 8386088 104
step cmd set 0 32 2015551 r93 - 0 0
step cmd set 0 10 823 past - 0 0
step cmd expire 0 6 - 0
step cmd setrange 0 121 424 204
step cmd set 1 9 894 r3547 - 0 0
step cmd set 0 10 1944 - - 0 0
step cmd set 0 6 428 - - 0 0
step cmd getdel 0 13
step cmd get 0 29
step cmd set 0 3 51 r177 - 0 0
step cmd set 0 85 2503 - - 0 0
step cmd get 0 11
step cmd mset 0 1:19,26:1280,51:597
step cmd set 0 27 27992 - - 0 0
step cmd get 0 104
step cmd get 0 91
step cmd expire 0 68 - 1
step cmd append 0 3 210
step cmd getdel 0 10
step cmd barrier
step cmd rename 0 2 102 0
step cmd set 0 79 278335 r4114 - 0 0
step cmd del 1 5
step cmd setrange 0 13 1020 410
step cmd get 0 118
step cmd set 1 64 451 r183 nx 0 0
step cmd msetnx 0 11:3878879,86:29
step cmd expire 0 5 r4953 0
step cmd msetnx 0 7:1200,82:1091
step ticks 56
step cmd set 0 1 2841 r1765 - 0 0
step cmd setrange 0 6 480 576
step ticks 4
step ticks 45
step cmd get 0 28
step cmd set 1 29 40419 - xx 0 0
step cmd meta 0 34
step cmd msetnx 1 0:751462,0:22
step cmd mset 0 56:6592,31:1498,31:1493
step cmd del 0 6,6,56
step cmd get 0 109
step cmd scan 0 0 10
step cmd set 0 9 3719 r1594 - 0 0
step cmd set 0 9 1343 - nx 0 0
step cmd flush 0
step cmd set 0 49 386 - - 0 0
step cmd set 0 67 1035 - - 0 0
step cmd rename 0 3 53 0
step cmd mset 0 3:2547,3:28
step cmd set 0 6 13 r161 - 0 0
step cmd setrange 0 88 228 368
step cmd expire 0 114 - 0
step cmd set 1 80 29323 - - 0 0
step cmd mset 0 8:2137,8:2021,58:19
step cmd msetnx 0 8:123,8:10754056,58:20,58:1053
step cmd set 0 10 367 - - 0 0
step cmd get 0 21
step cmd setrange 1 39 858 580
step cmd set 0 9 366 - - 0 0
step cmd getdel 0 7
step ticks 25
step cmd set 0 73 34248 - - 0 0
step cmd set 0 123 330 r81625664 - 0 0
step cmd append 0 13 198
step cmd setrange 0 109 5488 561
step cmd msetnx 0 13:1169,113:987,13:3169,38:500
step cmd getdel 0 12
step cmd set 1 14 252 - - 0 1
step cmd get 0 21
step cmd setrange 0 30 39415 346
step cmd set 0 126 2028 - - 0 0
step cmd set 0 14 293 - - 0 0
step cmd setrange 0 1 907 455
step cmd expire 0 56 - 0
step cmd append 0 5 5
step cmd scan 0 4 13
step cmd mset 1 11:1270,111:281547,86:79,61:177
step cmd expire 0 112 r2853 1
step cmd set 1 126 400 - nx 0 0
step cmd rename 0 71 46 1
step cmd set 0 67 0 - - 0 1
step cmd rename 1 9 59 0
step cmd get 0 14
step cmd append 0 12 160
step cmd setrange 0 8 655 363
step cmd set 0 1 1224 - - 1 0
step cmd set 0 95 276 - - 0 0
step ticks 6
step cmd del 0 28,3
step cmd del 0 4
step cmd rename 0 5 80 0
step cmd append 0 12 195
step cmd set 0 1 17 - nx 0 0
step cmd get 1 49
step cmd set 0 101 2065 r68 - 0 0
step clock 1290
step cmd setrange 0 13 499 69
step ticks 4
step cmd set 0 4 12 - - 0 0
step cmd del 0 115,40
step cmd get 1 7
step cmd get 1 7
step cmd meta 0 8
step cmd set 0 1 2216 - nx 0 0
step ticks 8
step cmd set 0 7 787 r827 nx 1 0
step cmd set 0 10 11 r152 - 0 0
step cmd set 0 14 1178 - - 0 0
step cmd expire 0 121 - 0
step cmd msetnx 0 8:744,8:41828,108:1176
step cmd del 0 9,109
step cmd msetnx 0 3:9519166,78:5
step cmd copy 0 14 39 1
step cmd flush 0
step cmd set 0 82 2018 - - 0 1
step cmd expire 0 103 r20 1
step cmd get 0 105
step cmd mset 0 102:534,77:1444,27:1146
step cmd set 0 85 851 - nx 1 0
step cmd set 1 3 1960 - - 0 0
step cmd append 0 14 119
"#,
    ),
    (
        "snapshot_written_after_failed_cutoff",
        r#"kv-sim storage case 1
seed 4728
fail I10 3 ioerr_clean_restart
cfg partitions 64
cfg devices 1
cfg ags 150
cfg lba 4096
cfg page_mode atomic
cfg atomic_unit 8192
cfg cache wb
cfg dsync 0
cfg durability async
cfg cache_bytes 67108864
cfg fill_pct 95
cfg snapshot 1
cfg keys 31
cfg groups 7
cfg dbs 1
cfg profile churn
cfg steps 1315
cfg rounds 5
cfg tick_permille 450
cfg delay_max 0
cfg late_pct 24
cfg skip_pct 10
cfg per_poll 0
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 2000
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd mset 0 24:0
step ticks 1
step cmd flush 0
step clean
"#,
    ),
    (
        "reclaim_header_read_overtaken_by_reuse",
        r#"kv-sim storage case 1
seed 219
fail ENGINE 892 GET-corrupt
cfg partitions 128
cfg devices 1
cfg ags 260
cfg lba 4096
cfg page_mode two-image
cfg atomic_unit -
cfg cache wb
cfg dsync 0
cfg durability flushed
cfg cache_bytes 1048576
cfg fill_pct 85
cfg snapshot 1
cfg keys 30
cfg groups 30
cfg dbs 1
cfg profile full
cfg steps 1366
cfg rounds 1
cfg tick_permille 250
cfg delay_max 32
cfg late_pct 37
cfg skip_pct 40
cfg per_poll 8
cfg shuffle 0
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 0
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 0 14 1923376 r53 - 0 0
step cmd set 0 0 1025 r123 - 0 0
step ticks 3
step ticks 2
step ticks 3
step cmd set 0 2 62245 - - 0 0
step cmd flush 0
step ticks 8
step cmd set 0 1 113087 - - 0 0
step ticks 17
step cmd set 0 1 6771336 - - 0 0
step cmd set 0 1 10241 - - 0 0
step cmd set 0 25 50003 - xx 0 0
step cmd append 0 0 197
step cmd del 0 0,0,0
step cmd del 0 26,26,26
step cmd set 0 22 275819 - xx 0 0
step ticks 8
step ticks 6
step cmd get 0 21
step cmd append 0 18 20
step ticks 8
step cmd mset 0 16:7833701,16:785935,16:65473
step ticks 44
step cmd copy 0 2 2 0
step cmd setrange 0 1 1138 79
step cmd set 0 0 763 - - 0 0
step ticks 5
step cmd set 0 0 570911 r94 - 0 0
step cmd del 0 0,0,0
step cmd get 0 12
step ticks 7
step cmd del 0 1
step ticks 4
step ticks 1
step cmd set 0 2 958430 - - 0 0
step ticks 1
step cmd get 0 21
step cmd del 0 25
step ticks 5
step cmd set 0 25 7797667 r142 - 0 0
step ticks 7
step cmd expire 0 1 - 1
step ticks 6
step cmd expire 0 22 - 1
step cmd del 0 26
step ticks 23
step cmd get 0 9
step cmd mset 0 23:38211,23:750,23:11795,23:63788
step cmd set 0 2 39757 r49963202 nx 0 0
step cmd meta 0 1
step ticks 5
step ticks 36
step ticks 4
step cmd persist 0 23
step cmd persist 0 2
step ticks 2
step cmd set 0 1 13205 r109 - 0 0
step cmd set 0 2 451541 r1606 - 1 0
step cmd set 0 2 109030 - - 0 0
step cmd del 0 24,24,24
step cmd set 0 16 5546217 - - 0 0
step cmd rename 0 1 1 1
step cmd del 0 1,1,1
step ticks 5
step cmd set 0 14 437474 - - 0 0
step cmd append 0 2 14
step ticks 8
step ticks 30
step cmd msetnx 0 2:192,2:830348,2:1156
step cmd append 0 1 209
step ticks 2
step cmd append 0 5 77
step cmd set 0 17 1007127 r82690878 - 0 0
step ticks 5
step ticks 12
step cmd set 0 10 52042 past - 0 0
step ticks 1
step ticks 3
step ticks 7
step ticks 50
step cmd append 0 1 258
step cmd del 0 1,1,1
step cmd del 0 19
step ticks 4
step cmd set 0 19 986 r2416 nx 0 0
step ticks 5
step cmd get 0 0
step cmd msetnx 0 1:31401,1:30296,1:638011,1:583
step cmd set 0 19 596994 r98 - 0 0
step cmd set 0 15 643497 - - 0 0
step cmd del 0 0,0,0
step cmd set 0 27 791 - xx 0 0
step cmd set 0 1 180 - nx 0 0
step ticks 2
step cmd setrange 0 26 898 393
step cmd set 0 14 28413 - - 1 0
step cmd expire 0 1 r99 0
step cmd msetnx 0 13:120877,13:606950,13:60066,13:479
step cmd setrange 0 15 237 471
step cmd del 0 12,12,12
step cmd set 0 24 447383 - - 0 0
step ticks 4
step cmd del 0 25,25
step cmd del 0 2,2,2
step cmd getdel 0 21
step cmd del 0 13
step cmd del 0 6,6
step cmd persist 0 2
step ticks 14
step ticks 2
step cmd set 0 2 930050 r102 - 0 0
step cmd barrier
step cmd set 0 1 671524 - - 0 0
step cmd set 0 0 1418 - - 0 0
step cmd setrange 0 29 1151 56
step cmd del 0 0,0,0
step cmd set 0 5 875 r64 - 0 0
step cmd set 0 2 48198 - - 0 0
step cmd set 0 0 191094 r4 - 0 0
step cmd set 0 1 993 - - 0 0
step ticks 3
step cmd del 0 4,4,4
step ticks 5
step cmd set 0 27 8342164 - - 0 0
step ticks 4
step cmd set 0 26 753425 - - 0 0
step cmd del 0 2,2,2
step cmd get 0 19
step cmd del 0 10,10
step clock 33
step cmd set 0 14 556474 r92 nx 0 0
step ticks 2
step ticks 17
step cmd set 0 27 85668 - nx 0 0
step cmd mset 0 21:199,21:1328,21:603
step ticks 7
step cmd get 0 2
step ticks 3
step cmd get 0 24
step ticks 8
step cmd get 0 1
step cmd del 0 24
step cmd setrange 0 9 316 442
step ticks 5
step cmd msetnx 0 5:1473,5:4425237,5:4873528
step cmd expire 0 17 past 1
step cmd set 0 0 434736 r4042 - 0 0
step cmd set 0 6 4121774 - - 0 0
step cmd del 0 15,15,15
step ticks 19
step cmd get 0 12
step cmd set 0 13 330630 - nx 0 0
step cmd expire 0 20 r317 0
step cmd set 0 1 34315 r35322958 - 0 0
step cmd set 0 1 15010 r152 - 0 1
step ticks 6
step ticks 2
step ticks 26
step cmd set 0 13 184 r1784 - 0 0
step cmd set 0 26 651156 r168 - 0 0
step cmd scan 0 14 16
step ticks 6
step cmd set 0 2 7165 - - 0 0
step cmd mset 0 29:49888,29:4844926,29:246597
step cmd barrier
step cmd del 0 2
step cmd set 0 22 9690 r34731646 - 1 0
step cmd set 0 2 1301 - - 0 0
step cmd append 0 28 272
step ticks 8
step ticks 53
step cmd setrange 0 7 320 387
step cmd get 0 4
step cmd del 0 7
step ticks 6
step ticks 4
step ticks 8
step cmd set 0 13 46713 - - 0 0
step ticks 43
step ticks 25
step cmd setrange 0 1 1038 521
step cmd del 0 2
step cmd scan 0 14 10
step cmd del 0 1,1
step clock 17
step ticks 42
step cmd getdel 0 11
step cmd get 0 0
step cmd getdel 0 1
step cmd set 0 20 769051 - - 0 0
step cmd getdel 0 1
step ticks 6
step cmd del 0 11,11,11
step cmd expire 0 3 r152 1
step cmd append 0 1 224
step ticks 8
step cmd get 0 2
step cmd set 0 0 490018 r133 nx 0 0
step cmd set 0 3 10057 - - 0 0
step cmd get 0 0
step cmd get 0 6
step cmd msetnx 0 8:29060,8:465611,8:1326,8:841863
step ticks 33
step ticks 4
step cmd mset 0 1:583355,1:618766,1:907217
step ticks 2
step cmd setrange 0 16 825 144
step ticks 3
step ticks 4
step cmd expire 0 1 - 0
step cmd get 0 22
step cmd del 0 2
step cmd set 0 27 268615 r103 xx 0 0
step cmd rename 0 6 6 0
step cmd set 0 0 938 - nx 0 0
step cmd set 0 1 765530 r30140588 nx 0 0
step ticks 6
step cmd set 0 2 30968 past xx 1 0
step ticks 3
step cmd del 0 0,0,0
step cmd del 0 0,0,0
step ticks 57
step cmd setrange 0 26 1173 173
step cmd getdel 0 22
step cmd msetnx 0 2:468,2:1003512,2:1035161,2:48102
step cmd set 0 1 844 r98 - 1 0
step cmd set 0 11 6426679 - - 0 0
step ticks 25
step cmd append 0 5 209
step cmd meta 0 2
step cmd set 0 23 386936 - - 0 0
step cmd del 0 0,0
step cmd get 0 0
step ticks 4
step cmd setrange 0 1 191 490
step cmd get 0 20
step ticks 6
step cmd del 0 1,1,1
step cmd append 0 0 271
step cmd set 0 7 48423 - - 1 0
step cmd set 0 2 585 r3442 - 0 0
step cmd barrier
step cmd del 0 0,0,0
step cmd del 0 17
step cmd set 0 27 730737 - - 0 0
step cmd del 0 1,1,1
step cmd set 0 18 117 r132 - 0 0
step cmd del 0 23,23
step cmd setrange 0 26 1000 440
step cmd set 0 3 1466 r6449049 - 0 0
step cmd set 0 4 321 r1409 - 1 0
step ticks 2
step cmd set 0 25 1448 - - 0 0
step cmd set 0 23 4018216 r4668 - 0 0
step cmd expire 0 6 - 0
step cmd set 0 11 801 - - 1 0
step ticks 7
step cmd set 0 2 522564 r85 - 1 0
step cmd barrier
step cmd set 0 1 853799 - - 0 0
step ticks 25
step cmd set 0 0 439664 past - 0 0
step ticks 55
step cmd msetnx 0 2:1356,2:482714
step cmd set 0 1 73295 r66 - 0 0
step cmd set 0 26 52857 r151 - 0 0
step cmd set 0 4 627 - - 0 0
step ticks 6
step ticks 7
step cmd set 0 15 1450 past xx 0 0
step cmd set 0 0 1342 - xx 0 0
step ticks 5
step ticks 4
step cmd meta 0 1
step cmd setrange 0 1 82 508
step cmd set 0 22 788678 r33 - 0 0
step ticks 8
step ticks 31
step cmd getdel 0 24
step cmd append 0 10 164
step cmd set 0 2 838195 - - 0 0
step ticks 5
step cmd del 0 4,4
step cmd get 0 0
step cmd getdel 0 20
step cmd set 0 1 1341 - nx 0 0
step cmd append 0 2 153
step cmd set 0 1 526006 r54863387 - 1 0
step cmd msetnx 0 2:241,2:22884,2:816946
step cmd set 0 7 231197 r179 - 0 0
step ticks 8
step cmd append 0 2 22
step ticks 3
step ticks 6
step cmd set 0 21 785440 - nx 0 0
step ticks 43
step cmd set 0 18 4112833 - xx 0 0
step ticks 3
step cmd set 0 0 39756 - - 0 0
step ticks 5
step cmd meta 0 0
step cmd set 0 15 229706 r39 - 1 0
step cmd append 0 9 162
step cmd set 0 5 60751 - - 0 0
step cmd get 0 1
step cmd set 0 29 736200 - nx 0 0
step cmd set 0 29 37709 - xx 0 0
step cmd del 0 11,11,11
step ticks 18
step ticks 8
step cmd set 0 20 248538 - - 0 0
step cmd get 0 13
step cmd persist 0 25
step ticks 34
step cmd set 0 1 62465 - - 0 0
step cmd set 0 2 16582 - - 0 0
step ticks 5
step cmd set 0 1 1133 r4575 - 0 0
step cmd del 0 13
step cmd set 0 0 339497 - - 1 0
step cmd rename 0 3 3 0
step cmd set 0 2 1342464 r3029 - 0 0
step ticks 4
step cmd setrange 0 9 597 574
step cmd del 0 1,1,1
step ticks 1
step cmd set 0 4 12408 r42 - 0 0
step cmd set 0 5 423 - - 0 0
step cmd del 0 14,14
step cmd del 0 22,22,22
step ticks 3
step ticks 4
step cmd del 0 25
step cmd getdel 0 1
step cmd set 0 23 614 r10675270 - 0 0
step cmd set 0 3 701208 - nx 0 0
step cmd set 0 2 7626 r193 - 0 0
step cmd setrange 0 19 973 364
step ticks 3
step ticks 6
step ticks 2
step cmd set 0 28 59947 - - 0 0
step cmd append 0 2 242
step ticks 5
step cmd get 0 20
step ticks 6
step cmd meta 0 0
step cmd set 0 26 33018 - - 0 0
step cmd del 0 0
step ticks 6
step cmd set 0 6 6017823 - - 0 0
step cmd del 0 1,1,1
step cmd del 0 2,2,2
step cmd set 0 22 768651 r4905 - 0 0
step cmd del 0 0
step cmd expire 0 19 r13 0
step cmd set 0 29 53127 past - 0 0
step cmd getdel 0 29
step cmd setrange 0 18 2187 40
step cmd set 0 14 1004423 - - 0 0
step ticks 2
step ticks 6
step ticks 1
step cmd del 0 29
step cmd del 0 0,0,0
step cmd set 0 3 52132 r104 - 0 0
step cmd set 0 17 978122 - - 0 0
step cmd set 0 17 620627 r94 - 0 0
step cmd copy 0 22 22 1
step cmd set 0 1 595358 r4032 - 0 0
step cmd set 0 2 118057 r170 - 0 0
step cmd expire 0 1 - 0
step ticks 1
step cmd get 0 0
step cmd get 0 5
step cmd append 0 13 248
step ticks 10
step ticks 3
step cmd del 0 29,29
step cmd get 0 1
step cmd set 0 1 923146 r159 - 0 0
step cmd expire 0 1 - 0
step cmd set 0 18 665338 past nx 0 1
step cmd set 0 13 764 - - 0 0
step ticks 2
step ticks 59
step cmd get 0 2
step cmd set 0 26 274865 - - 0 0
step cmd set 0 19 1486 r1038 - 1 0
step cmd del 0 16
step ticks 5
step ticks 8
step clock 29
step ticks 5
step cmd set 0 0 859 - - 1 0
step cmd del 0 11,11,11
step cmd get 0 10
step cmd get 0 12
step cmd set 0 0 894284 - - 0 0
step cmd get 0 6
step cmd get 0 16
step ticks 4
step cmd set 0 24 575 - - 0 0
step cmd set 0 13 181101 - - 1 0
step cmd set 0 21 430897 r1776 nx 1 0
step cmd set 0 1 356362 - - 0 0
step cmd del 0 25,25
step ticks 1
step ticks 7
step cmd del 0 19,19,19
step cmd set 0 1 2748299 - - 0 0
step ticks 8
step cmd del 0 20,20,20
step cmd set 0 1 32415 - - 0 0
step cmd msetnx 0 2:16494,2:507642,2:539,2:763266
step ticks 6
step cmd getdel 0 0
step cmd mset 0 0:776057,0:826559,0:1493
step ticks 7
step cmd set 0 25 439130 r156 - 0 0
step cmd del 0 17,17
step cmd set 0 2 234704 r57 - 0 0
step cmd set 0 1 47312 - - 0 1
step cmd flush 0
step cmd meta 0 16
step ticks 3
step ticks 14
step cmd mset 0 26:312,26:19490,26:886570,26:15235
step cmd del 0 0,0,0
step cmd get 0 2
step cmd msetnx 0 27:318,27:472094,27:21946
step cmd get 0 9
step cmd del 0 0,0
step cmd del 0 2
step cmd setrange 0 3 174 193
step ticks 7
step cmd set 0 2 63701 r766 - 0 0
step cmd set 0 18 490905 - - 0 0
step cmd set 0 4 550664 - - 0 0
step ticks 3
step ticks 7
step cmd append 0 1 184
step cmd set 0 1 28543 - - 0 0
step ticks 1
step ticks 31
step cmd mset 0 2:7852,2:4005492
step cmd del 0 1,1,1
step cmd set 0 26 787165 - - 0 0
step cmd del 0 2,2,2
step cmd set 0 6 2974472 - - 1 0
step cmd set 0 2 9827 - nx 0 0
step cmd set 0 16 2338332 - nx 0 0
step ticks 2
step cmd rename 0 17 17 0
step cmd get 0 16
step cmd mset 0 3:34180,3:53863
step cmd expire 0 2 - 0
step cmd set 0 2 838326 - - 0 0
step cmd set 0 14 246403 r124 - 0 0
step cmd set 0 0 53 r32213187 - 0 0
step cmd set 0 1 279826 r1424 nx 0 0
step ticks 8
step cmd get 0 0
step cmd set 0 0 216487 - nx 0 0
step cmd get 0 29
step cmd append 0 21 22
step cmd getdel 0 2
step cmd append 0 7 274
step ticks 5
step cmd setrange 0 21 710 258
step cmd setrange 0 28 1037 504
step cmd set 0 19 561 - nx 0 0
step cmd del 0 29
step ticks 55
step ticks 4
step cmd del 0 1,1,1
step ticks 5
step cmd del 0 27,27
step cmd set 0 1 527047 - xx 1 0
step ticks 26
step cmd getdel 0 5
step cmd del 0 1,1
step cmd set 0 9 9224 past - 0 0
step ticks 3
step cmd get 0 26
step cmd copy 0 1 1 1
step cmd get 0 2
step cmd get 0 1
step cmd set 0 0 102403 - nx 0 0
step cmd del 0 0
step cmd setrange 0 7 1059 315
step cmd set 0 1 795427 - xx 0 0
step ticks 57
step cmd set 0 0 168 - - 0 0
step cmd del 0 3,3,3
step cmd setrange 0 6 941 247
step cmd set 0 18 32286 past - 0 0
step cmd meta 0 22
step cmd get 0 18
step cmd rename 0 21 21 0
step cmd set 0 19 45560 - - 0 0
step cmd expire 0 8 - 1
step ticks 1
step ticks 2
step ticks 18
step cmd del 0 4,4
step cmd rename 0 1 1 1
step cmd set 0 13 785042 - - 0 0
step ticks 5
step cmd rename 0 1 1 0
step ticks 7
step quiesce
step cmd del 0 2,2
step ticks 7
step cmd set 0 0 1015145 - - 0 0
step cmd set 0 2 161 - - 0 0
step quiesce
step cmd mset 0 15:879875,15:600447,15:752745
step cmd set 0 2 433901 r121 - 0 0
step cmd set 0 28 53970 r5716331 - 0 0
step cmd set 0 20 348575 r4085 - 0 0
step ticks 7
step cmd del 0 19,19,19
step cmd mset 0 19:436408,19:404743
step cmd append 0 0 22
step ticks 33
step cmd rename 0 29 29 0
step cmd rename 0 0 0 0
step ticks 58
step cmd set 0 17 502377 past - 0 0
step cmd set 0 28 93593 - - 0 1
step cmd del 0 1,1,1
step ticks 2
step cmd set 0 7 847699 r15096351 - 0 0
step cmd append 0 11 82
step cmd append 0 0 248
step cmd rename 0 28 28 0
step cmd get 0 10
step cmd del 0 1,1
step cmd set 0 1 852606 - - 0 0
step cmd del 0 11,11,11
step ticks 2
step cmd mset 0 8:845,8:17120,8:14315,8:11038
step cmd set 0 3 955 - xx 0 0
step cmd setrange 0 0 25943 524
step ticks 5
step cmd set 0 15 41936 - - 0 0
step cmd expire 0 7 - 1
step cmd del 0 0,0,0
step ticks 37
step cmd del 0 1,1
step cmd set 0 17 16304 - - 0 0
step cmd del 0 1,1
step ticks 8
step cmd get 0 11
step ticks 1
step ticks 3
step cmd get 0 0
step ticks 43
step cmd expire 0 15 r54 1
step cmd expire 0 13 - 1
step ticks 1
step cmd set 0 24 626570 past - 0 0
step cmd set 0 4 707 - - 0 0
step ticks 45
step cmd append 0 17 293
step cmd set 0 21 332239 - - 0 0
step cmd set 0 26 1430 - - 0 0
step ticks 3
step cmd set 0 20 151413 - - 0 0
step ticks 43
step ticks 7
step cmd set 0 28 1193 - - 0 0
step ticks 4
step cmd get 0 1
step ticks 8
step ticks 3
step ticks 5
step cmd set 0 2 539910 - - 0 0
step ticks 29
step cmd get 0 8
step ticks 7
step cmd del 0 25,25,25
step cmd rename 0 5 5 0
step cmd set 0 4 4170587 r83 - 0 0
step ticks 3
step ticks 8
step cmd expire 0 0 r37847088 0
step ticks 6
step cmd del 0 27
step cmd set 0 0 14203 r3486 nx 0 0
step ticks 7
step cmd rename 0 0 0 0
step cmd set 0 23 1231 - - 0 0
step cmd set 0 23 878779 r42 - 1 0
step ticks 3
step ticks 2
step cmd del 0 2
step cmd set 0 2 7642167 - - 0 0
step cmd set 0 16 1324814 r148 - 0 0
step cmd append 0 9 208
step cmd set 0 1 23227 - - 0 0
step ticks 4
step cmd set 0 1 561734 r3742 - 1 0
step cmd del 0 0,0,0
step cmd set 0 6 398264 r59629883 - 0 0
step cmd rename 0 5 5 0
step cmd meta 0 17
step cmd set 0 23 52926 - - 0 0
step ticks 6
step cmd set 0 1 37285 - xx 1 0
step ticks 7
step ticks 5
step ticks 38
step cmd msetnx 0 21:1237,21:134565,21:1423,21:827590
step cmd barrier
step ticks 5
step cmd mset 0 14:118,14:48621,14:702
step cmd set 0 0 989795 - - 0 0
step cmd getdel 0 14
step ticks 16
step cmd del 0 16
step cmd del 0 17,17,17
step cmd del 0 17,17
step cmd del 0 2,2,2
step cmd copy 0 5 5 1
step cmd set 0 1 981682 - - 0 0
step ticks 8
step cmd rename 0 2 2 0
step cmd set 0 5 353006 r2901 nx 0 0
step cmd set 0 26 884677 - - 0 0
step cmd setrange 0 28 19430 559
step ticks 7
step cmd set 0 7 32523 r117 - 0 0
step cmd set 0 2 259 - - 0 0
step cmd msetnx 0 1:29958,1:8518
step cmd del 0 7,7,7
step cmd set 0 0 967537 - - 0 0
step cmd scan 0 53 12
step cmd append 0 7 287
step ticks 3
step cmd del 0 28,28,28
step ticks 5
step cmd del 0 16,16,16
step cmd setrange 0 2 37 387
step cmd set 0 0 12140 r16804819 - 0 0
step ticks 4
step ticks 2
step cmd flush 0
step cmd get 0 6
step cmd persist 0 0
step ticks 2
step cmd set 0 29 609934 - - 0 0
step cmd copy 0 3 3 1
step cmd set 0 10 289 - - 0 0
step cmd setrange 0 1 280 580
step ticks 5
step cmd set 0 2 471 - - 0 0
step cmd del 0 29
step cmd msetnx 0 18:865340,18:28176
step cmd set 0 28 120686 r3331 - 0 0
step cmd del 0 17
step cmd set 0 2 250400 - - 0 0
step cmd scan 0 31 29
step ticks 6
step ticks 52
step cmd meta 0 25
step cmd del 0 2,2,2
step cmd set 0 0 40993 r1417 - 0 0
step cmd get 0 21
step cmd msetnx 0 0:236,0:698971,0:53577
step cmd del 0 3,3
step cmd set 0 20 36603 - - 0 0
step cmd set 0 10 1335833 - - 0 1
step cmd del 0 13,13,13
step cmd set 0 11 593303 r374 nx 0 0
step cmd set 0 1 523052 - - 0 0
step cmd set 0 19 578 - - 0 0
step cmd set 0 25 8601 - - 0 0
step cmd del 0 2,2,2
step cmd set 0 7 882306 - - 1 0
step clock 99201
step cmd set 0 2 54830 - - 0 0
step ticks 2
step cmd set 0 2 509099 r173 - 0 0
step ticks 3
step cmd del 0 1,1,1
step cmd set 0 0 51597 - - 0 0
step cmd set 0 26 2369820 - - 0 0
step cmd msetnx 0 2:431,2:33309,2:69736
step ticks 1
step ticks 1
step cmd del 0 1,1
step cmd set 0 1 188237 - xx 0 0
step cmd set 0 13 471444 r63520701 - 0 0
step ticks 6
step cmd del 0 3,3,3
step cmd msetnx 0 0:246641,0:5759275
step cmd set 0 2 935813 - - 0 0
step ticks 2
step cmd msetnx 0 20:35400,20:1221
step ticks 23
step ticks 8
step cmd del 0 26
step cmd del 0 13,13,13
step cmd append 0 4 191
step cmd set 0 2 1443 - - 1 1
step ticks 5
step ticks 4
step cmd get 0 1
step cmd set 0 0 10374 r39 - 0 0
step cmd set 0 10 416 r120 nx 0 0
step cmd append 0 6 249
step cmd msetnx 0 14:861,14:6449407,14:85849
step cmd set 0 11 649047 - - 0 0
step cmd set 0 0 359644 - - 0 0
step cmd set 0 21 245252 - - 0 0
step cmd set 0 10 631301 - - 0 0
step cmd set 0 0 366781 - - 0 0
step cmd mset 0 2:497636,2:41328,2:946460,2:569501
step ticks 4
step ticks 36
step cmd set 0 1 302205 - - 1 0
step cmd rename 0 2 2 0
step cmd mset 0 0:13804,0:86314,0:388694
step ticks 3
step cmd del 0 2,2
step ticks 41
step cmd barrier
step cmd set 0 2 48974 - - 0 0
step cmd set 0 29 23047 r60711979 - 0 0
step cmd meta 0 1
step cmd get 0 24
step ticks 4
step cmd rename 0 0 0 0
step cmd append 0 10 101
step cmd getdel 0 3
step cmd set 0 1 538582 - - 0 0
step cmd msetnx 0 0:33305,0:1201578,0:858887
step cmd del 0 0,0,0
step cmd del 0 0
step quiesce
step cmd set 0 1 43072 r92 - 0 0
step ticks 5
step cmd append 0 4 33
step ticks 8
step cmd set 0 0 25358 - - 0 0
step cmd expire 0 27 r4506 1
step cmd set 0 2 427461 r1884 - 0 0
step cmd msetnx 0 2:4114,2:457,2:956820,2:339385
step cmd set 0 11 459193 r2091 - 0 0
step cmd set 0 2 1215 r68595582 - 0 0
step ticks 6
step cmd del 0 6,6
step cmd set 0 28 61897 - xx 0 0
step ticks 2
step cmd del 0 0
step cmd del 0 28,28,28
step cmd meta 0 1
step cmd expire 0 2 past 1
step cmd get 0 17
step cmd set 0 18 11170 - - 0 0
step ticks 3
step cmd set 0 7 733693 - nx 0 0
step cmd del 0 2
step cmd del 0 14
step ticks 3
step cmd set 0 2 1034 - - 0 0
step cmd set 0 1 494204 - nx 0 0
step cmd del 0 0,0
step cmd get 0 1
step cmd set 0 0 35732 r3372 - 0 0
step ticks 1
step cmd set 0 2 599676 r6 - 0 0
step cmd del 0 2,2
step cmd scan 0 66 30
step cmd set 0 12 313341 - - 0 0
step cmd copy 0 29 29 1
step cmd meta 0 7
step cmd scan 0 6 26
step cmd scan 0 57 11
step ticks 5
step ticks 7
step ticks 12
step ticks 1
step cmd set 0 6 454420 - xx 0 1
step ticks 7
step cmd set 0 2 1478 r975 - 0 0
step cmd set 0 0 14052 - - 0 0
step cmd scan 0 92 1
step cmd set 0 6 715998 past - 0 0
step cmd set 0 1 550198 - - 0 0
step cmd mset 0 17:1365,17:574679
step ticks 28
step ticks 19
step ticks 6
step ticks 4
step cmd getdel 0 12
step cmd set 0 0 3144231 - - 1 1
step ticks 3
step ticks 7
step cmd del 0 5
step cmd del 0 22,22,22
step cmd get 0 0
step ticks 5
step ticks 43
step cmd set 0 13 894676 r195 - 0 1
step ticks 58
step cmd del 0 1
step cmd set 0 0 75949 r123 - 0 0
step ticks 5
step cmd del 0 25,25,25
step cmd set 0 26 29234 - - 1 0
step ticks 8
step cmd append 0 9 254
step ticks 9
step cmd mset 0 2:89,2:103,2:562
step cmd set 0 4 572265 - - 0 0
step ticks 5
step cmd append 0 3 220
step cmd set 0 0 41414 - xx 0 0
step ticks 27
step cmd set 0 26 881 r17 nx 1 0
step ticks 3
step cmd set 0 22 990543 - - 0 0
step cmd set 0 26 1012535 past - 0 0
step cmd getdel 0 5
step ticks 5
step cmd del 0 25,25
step ticks 2
step ticks 4
step cmd set 0 1 926 r18 - 1 0
step cmd append 0 3 62
step ticks 5
step ticks 5
step cmd msetnx 0 18:42775,18:267,18:1044281,18:362
step cmd del 0 1,1
step cmd set 0 17 768 r169 - 0 0
step cmd set 0 2 8767 r57460389 - 0 0
step ticks 2
step ticks 6
step cmd set 0 29 7619075 - xx 0 0
step ticks 56
step cmd mset 0 1:242313,1:208328,1:4711864
step cmd setrange 0 1 14969 148
step cmd get 0 1
step cmd msetnx 0 12:85,12:52035
step cmd del 0 2,2
step cmd set 0 18 6584514 r2383 - 0 0
step ticks 5
step cmd mset 0 1:1370,1:52572,1:24046
step cmd set 0 2 57414 r74 - 0 0
step cmd set 0 27 2309092 - - 0 0
step cmd set 0 25 621579 - nx 0 0
step cmd get 0 0
step cmd persist 0 2
step ticks 7
step cmd get 0 2
step ticks 13
step cmd set 0 6 884874 - - 1 0
step ticks 7
step cmd set 0 0 44624 - - 0 0
step cmd set 0 23 6114595 r84415279 - 1 0
step cmd set 0 0 851338 - - 0 0
step cmd set 0 19 99487 - xx 0 0
step ticks 3
step cmd del 0 20,20
step cmd set 0 0 4415 - - 0 0
step cmd set 0 14 10600 - nx 0 0
step clock 46
step cmd append 0 1 204
step cmd append 0 3 153
step cmd set 0 0 5176 - - 0 0
step cmd set 0 23 546 r1456 - 0 0
step cmd set 0 16 1299 - - 0 0
step ticks 8
step cmd del 0 0,0,0
step cmd set 0 1 24229 - - 0 0
step cmd set 0 14 27345 r751 xx 0 0
step cmd barrier
step cmd flush 0
step ticks 2
step ticks 1
step cmd mset 0 2:379512,2:1320
step cmd append 0 11 237
step cmd set 0 13 8229862 - - 0 0
step cmd set 0 27 1026888 - nx 0 0
step cmd scan 0 22 19
step cmd expire 0 21 - 1
step ticks 8
step cmd mset 0 0:6197174,0:5547,0:481514,0:6469183
step quiesce
"#,
    ),
    (
        "expired_rename_destination_of_an_uncommitted_txn",
        r#"kv-sim storage case 1
seed 1004315
fail I4 25 ioerr_power_cut-Power
cfg partitions 32
cfg devices 2
cfg ags 50
cfg lba 512
cfg page_mode two-image
cfg atomic_unit -
cfg cache wt
cfg dsync 0
cfg durability flushed
cfg cache_bytes 4194304
cfg fill_pct 95
cfg snapshot 1
cfg keys 12
cfg groups 3
cfg dbs 1
cfg profile large
cfg steps 263
cfg rounds 5
cfg tick_permille 120
cfg delay_max 200
cfg late_pct 47
cfg skip_pct 40
cfg per_poll 0
cfg shuffle 1
cfg tick_ms 1
cfg hang_ticks 20000
cfg settle_ticks 30000
cfg error_ppm 200
cfg no_flush 0
cfg max_len 0
cfg unit_cap 0
step cmd set 0 0 3820 - nx 0 0
step cmd set 0 0 11 r19593347 - 0 0
step ticks 3
step cmd copy 0 0 6 1
step cmd mset 0 1:1082,10:85122
step cmd append 0 0 8
step cmd scan 0 31 3
step cmd meta 0 6
step cmd getdel 0 0
step cmd meta 0 5
step cmd set 0 5 2427 - nx 0 0
step clean
step cmd barrier
step cmd set 0 0 84628 - nx 0 1
step cmd set 0 0 4772651 - - 0 0
step cmd set 0 10 14 r1441 - 0 0
step cmd setrange 0 11 1113 80
step cmd set 0 0 16 - - 0 0
step cmd expire 0 0 r4949 1
step ticks 1
step ticks 1
step cmd rename 0 0 6 0
step cmd set 0 5 16 - - 0 0
step ticks 7
step clock 5826
step crash power sectors 2
"#,
    ),
];

/// Replays a case text.
pub fn replay_case(text: &str) -> Result<RunStats, Failure> {
    let (cfg, steps, _) = parse_case(text).map_err(|e| Failure::new(FailKind::Engine, "bad-case", e))?;
    run_caught(&cfg, &steps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps_round_trip_through_text() {
        let cfg = StorageSimCfg::from_seed(7, &Filter::default());
        let steps = generate_steps(&cfg);
        let text = case_text(&cfg, &steps, None, "test");
        let (c2, s2, _) = parse_case(&text).unwrap();
        assert_eq!(c2, cfg);
        assert_eq!(s2, steps);
    }

    #[test]
    fn values_are_distinct_per_step() {
        assert_ne!(value(1, 0, 32), value(2, 0, 32));
        assert_ne!(value(1, 0, 32), value(1, 1, 32));
        assert_eq!(value(5, 0, 100), value(5, 0, 100));
    }

    /// Every minimized case of a known defect passes (fails until the defects are fixed).
    #[test]
    fn regression_cases_pass() {
        let mut fails = Vec::new();
        for (name, text) in REGRESSION_CASES {
            if let Err(f) = replay_case(text) {
                fails.push(format!("{name}: {}", f.line()));
            }
        }
        assert!(fails.is_empty(), "{} of {} regression cases fail:\n{}", fails.len(), REGRESSION_CASES.len(), fails.join("\n"));
    }

    #[test]
    fn regression_cases_parse() {
        for (name, text) in REGRESSION_CASES {
            let (_, steps, kind) = parse_case(text).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(!steps.is_empty() && kind.is_some(), "{name}");
        }
    }

    /// Smoke: 200 seeds of small runs (the CLI runs the full matrix).
    #[test]
    fn storage_sim_smoke_200_seeds() {
        let n: u64 = std::env::var("KV_SIM_SMOKE_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(200);
        let start: u64 = std::env::var("KV_SIM_SMOKE_START").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
        let f = Filter { smoke: true, ..Filter::default() };
        let mut fails = Vec::new();
        for seed in start..start + n {
            let cfg = StorageSimCfg::from_seed(seed, &f);
            if let Err(e) = run_caught(&cfg, &generate_steps(&cfg)) {
                fails.push(format!("seed {seed} ({}): {}", cfg.cell(), e.line()));
            }
        }
        assert!(fails.is_empty(), "{} of {n} smoke seeds failed (replay: kv-sim storage --show SEED --smoke):\n{}", fails.len(), fails.join("\n"));
    }
}
