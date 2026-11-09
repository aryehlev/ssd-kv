// ioredis verifier client of the cluster client matrix (DESIGN §25.6).
//
// Usage: node client.js --name NAME --ctrl DIR --seeds h:p,h:p,h:p
//
// Settings per DESIGN §23.5: none beyond the coordinator seeds (ioredis follows MOVED to the
// coordinator, then refreshes; harmless node-error events are ignored). Replica reads use
// `scaleReads: "slave"` (ioredis sends READONLY on replica connections).
//
// Phases: static checks (bootstrap, pipeline, MULTI on one slot, server CROSSSLOT) -> NAME.static;
// a verifier writer until DIR/stop; READONLY replica reads; verification of every
// acknowledged write -> NAME.result (JSON). Every wait is bounded.
"use strict";
const Redis = require("ioredis");
const fs = require("fs");
const path = require("path");

const args = {};
for (let i = 2; i < process.argv.length; i += 2) args[process.argv[i].replace(/^--/, "")] = process.argv[i + 1];
const name = args.name, ctrl = args.ctrl;
const seeds = args.seeds.split(",").map((s) => { const [host, port] = s.split(":"); return { host, port: Number(port) }; });
const pauseMs = Number(process.env.KV_MATRIX_WRITER_PAUSE_MS || "4");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ts = () => Date.now() / 1000;

function cluster(extra) {
  const c = new Redis.Cluster(seeds, {
    redisOptions: { connectTimeout: 2000, commandTimeout: 3000, maxRetriesPerRequest: 2 },
    retryDelayOnMoved: 0,
    retryDelayOnTryAgain: 50,
    retryDelayOnClusterDown: 100,
    slotsRefreshTimeout: 2000,
    ...extra,
  });
  c.on("error", () => {});
  c.on("node error", () => {});
  return c;
}

async function retry(deadlineMs, what, f) {
  let last;
  while (Date.now() < deadlineMs) {
    try { return await f(); } catch (e) { last = e; await sleep(100); }
  }
  throw new Error(`${what}: ${last}`);
}

// Per-key attempts of one sequential writer, and its client-visible outages per slot range
// (slot >> 12: four ranges aligned with every topology boundary of the matrix), so that
// writes to healthy shards never mask an outage of another shard.
const keySlot = require("cluster-key-slot");
// a gap between two successful writes of one slot range longer than this is an outage
const GAP_S = 0.25;
class History {
  constructor() { this.keys = new Map(); this.acked = 0; this.unacked = 0; this.outages = []; this.failSince = new Map(); this.lastOk = new Map(); }
  record(k, v, ok, started) {
    if (!this.keys.has(k)) this.keys.set(k, []);
    this.keys.get(k).push([v, ok]);
    const now = ts();
    const b = keySlot(k) >> 12;
    const last = this.lastOk.has(b) ? this.lastOk.get(b) : started;
    if (ok) {
      this.acked++;
      if (now - last > GAP_S || this.failSince.has(b)) this.outages.push([last, now, b]);
      this.failSince.delete(b);
      this.lastOk.set(b, now);
    } else {
      this.unacked++;
      if (!this.failSince.has(b)) this.failSince.set(b, last);
    }
  }
  // the last acknowledged value or any unacknowledged attempt (G6); null when nothing acked
  allowed(k) {
    const h = this.keys.get(k);
    let last = -1;
    h.forEach(([, ok], i) => { if (ok) last = i; });
    if (last < 0) return null;
    const s = new Set([h[last][0]]);
    for (const [v, ok] of h) if (!ok) s.add(v);
    return s;
  }
}

(async () => {
  const res = { name, lib: "ioredis", ok: false, checks: {} };
  const c = cluster({});
  let deadline = Date.now() + 60000;
  await retry(deadline, "bootstrap", () => c.ping());
  res.checks.bootstrap = "ok";

  const statics = [];
  for (let i = 0; i < 200; i++) statics.push([`${name}:s:${i}`, `s${i}-${name}`]);
  for (const [k, v] of statics) await retry(deadline, `SET ${k}`, () => c.set(k, v));
  for (const [k, v] of statics) {
    const got = await retry(deadline, `GET ${k}`, () => c.get(k));
    if (got !== v) throw new Error(`GET ${k}: ${got}`);
  }

  // pipeline: ioredis requires the keys of one cluster pipeline to share a slot (hashtag)
  const ptag = `{${name}-p}`;
  const out = await retry(deadline, "pipeline", async () => {
    const p = c.pipeline();
    for (let i = 0; i < 100; i++) p.set(`${ptag}:${i}`, `v${i}`);
    for (let i = 0; i < 100; i++) p.get(`${ptag}:${i}`);
    return p.exec();
  });
  for (let i = 0; i < 100; i++) {
    if (out[i][0] || out[i][1] !== "OK") throw new Error(`pipeline SET ${i}`);
    if (out[100 + i][0] || out[100 + i][1] !== `v${i}`) throw new Error(`pipeline GET ${i}`);
  }
  res.checks.pipeline = "ok (100 SET + 100 GET, one slot)";

  // MULTI/EXEC on one slot
  const tag = `{${name}-tx}`;
  const tx = await retry(deadline, "MULTI/EXEC", () => c.multi().set(`${tag}:a`, "1").incr(`${tag}:a`).set(`${tag}:b`, "x").get(`${tag}:a`).exec());
  if (JSON.stringify(tx.map((r) => r[1])) !== JSON.stringify(["OK", 2, "OK", "2"])) throw new Error(`MULTI/EXEC replies ${JSON.stringify(tx)}`);
  res.checks.multi = "ok (cluster multi)";

  // CROSSSLOT: ioredis sends MSET to the owner of the first key; the server refuses it
  const k1 = `${name}:x:a`, k2 = `${name}:x:b`;
  let cross = null;
  try { await c.mset(k1, "1", k2, "2"); } catch (e) { cross = String(e.message || e); }
  if (cross === null) throw new Error("cross-slot MSET was accepted");
  if (!/CROSSSLOT|same slot/i.test(cross)) throw new Error(`cross-slot MSET: ${cross}`);
  if ((await c.get(k1)) !== null) throw new Error("the refused MSET wrote a key");
  res.checks.crossslot = /^CROSSSLOT/.test(cross) ? "ok (server CROSSSLOT)" : `ok (client refused: ${cross})`;
  fs.writeFileSync(path.join(ctrl, `${name}.static`), JSON.stringify(res.checks));
  console.log(`[${name}] static checks passed`);

  // verifier writer
  const h = new History();
  const stop = path.join(ctrl, "stop");
  const hardEnd = Date.now() + 900000;
  let seed = 12345;
  const rnd = (n) => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed % n; };
  for (let i = 1; !fs.existsSync(stop) && Date.now() < hardEnd; i++) {
    const k = i > 10 && i % 5 === 0 ? `${name}:w:${1 + rnd(Math.floor(i / 5) * 4 - 1)}` : `${name}:w:${i}`;
    const v = `${name}-${i}`;
    let ok = false;
    const t0 = ts();
    try { ok = (await c.set(k, v)) === "OK"; } catch (e) { ok = false; }
    h.record(k, v, ok, t0);
    if (pauseMs > 0) await sleep(pauseMs);
  }
  for (const [b, t] of h.failSince) h.outages.push([t, ts(), b]);
  res.acked = h.acked; res.unacked = h.unacked; res.outages = h.outages;
  console.log(`[${name}] writer done: ${h.acked} acked, ${h.unacked} not acknowledged`);

  // READONLY replica reads
  const ro = cluster({ scaleReads: "slave" });
  deadline = Date.now() + 60000;
  let stale = 0;
  for (const [k, v] of statics) {
    const end = Date.now() + 10000;
    for (;;) {
      const got = await retry(deadline, `replica GET ${k}`, () => ro.get(k));
      if (got === v) break;
      if (Date.now() > end) throw new Error(`replica read of ${k}: ${got}`);
      stale++;
      await sleep(50);
    }
  }
  res.checks.readonly = `ok (${statics.length} replica reads, ${stale} stale retries)`;

  // zero lost acknowledged writes
  const lost = [];
  deadline = Date.now() + 300000;
  for (const k of h.keys.keys()) {
    const allowed = h.allowed(k);
    if (allowed === null) continue;
    const got = await retry(deadline, `verify GET ${k}`, () => c.get(k));
    if (!allowed.has(got)) lost.push(`${k}=${got}`);
  }
  res.lost = lost.length; res.lost_examples = lost.slice(0, 5); res.ok = lost.length === 0;
  fs.writeFileSync(path.join(ctrl, `${name}.result`), JSON.stringify(res));
  console.log(`[${name}] verify: ${lost.length} lost of ${h.acked} acknowledged`);
  c.disconnect(); ro.disconnect();
  process.exit(lost.length ? 1 : 0);
})().catch((e) => { console.log(`FAILED: ${e && e.stack ? e.stack : e}`); process.exit(1); });
