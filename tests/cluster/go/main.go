// go-redis verifier client of the cluster client matrix (DESIGN §25.6).
//
// Usage: goclient --name NAME --ctrl DIR --seeds h:p,h:p,h:p [--proto 2|3]
//
// Settings per DESIGN §23.5: a ClusterSlots function that queries the coordinators, MOVED
// handled by go-redis (lazy reload, MaxRedirects 3), TRYAGAIN/CLUSTERDOWN retried by go-redis;
// replica reads with ReadOnly (go-redis sends READONLY on replica connections).
//
// Phases: static checks (bootstrap, pipeline, MULTI on one slot, server CROSSSLOT) -> NAME.static;
// a verifier writer until DIR/stop; READONLY replica reads; verification of every
// acknowledged write -> NAME.result (JSON). Every wait is bounded.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type attempt struct {
	v  string
	ok bool
}

// history: per-key attempts of one sequential writer, and its client-visible outages per
// slot range (slot >> 12: four ranges aligned with every topology boundary of the matrix), so
// that writes to healthy shards never mask an outage of another shard.
type history struct {
	keys      map[string][]attempt
	order     []string
	acked     int
	unacked   int
	outages   [][3]float64
	failSince map[int]float64
	lastOK    map[int]float64
}

func ts() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// crc16 is CRC16-XMODEM, the Redis Cluster key slot hash.
func crc16(b []byte) uint16 {
	var c uint16
	for _, x := range b {
		c ^= uint16(x) << 8
		for i := 0; i < 8; i++ {
			if c&0x8000 != 0 {
				c = c<<1 ^ 0x1021
			} else {
				c <<= 1
			}
		}
	}
	return c
}

func keySlot(k string) int {
	if s := strings.IndexByte(k, '{'); s >= 0 {
		if e := strings.IndexByte(k[s+1:], '}'); e > 0 {
			k = k[s+1 : s+1+e]
		}
	}
	return int(crc16([]byte(k)) & 16383)
}

// gapS: a gap between two successful writes of one slot range longer than this is an outage
// (failed writes in between, or one call that blocked).
const gapS = 0.25

func (h *history) record(k, v string, ok bool, started float64) {
	if _, seen := h.keys[k]; !seen {
		h.order = append(h.order, k)
	}
	h.keys[k] = append(h.keys[k], attempt{v, ok})
	now := ts()
	b := keySlot(k) >> 12
	last, seen := h.lastOK[b]
	if !seen {
		last = started
	}
	if ok {
		h.acked++
		_, failing := h.failSince[b]
		if now-last > gapS || failing {
			h.outages = append(h.outages, [3]float64{last, now, float64(b)})
		}
		delete(h.failSince, b)
		h.lastOK[b] = now
	} else {
		h.unacked++
		if _, failing := h.failSince[b]; !failing {
			h.failSince[b] = last
		}
	}
}

// allowed: the last acknowledged value or any unacknowledged attempt (G6); nil when nothing
// was acknowledged.
func (h *history) allowed(k string) map[string]bool {
	a := h.keys[k]
	last := -1
	for i, x := range a {
		if x.ok {
			last = i
		}
	}
	if last < 0 {
		return nil
	}
	m := map[string]bool{a[last].v: true}
	for _, x := range a {
		if !x.ok {
			m[x.v] = true
		}
	}
	return m
}

func retry[T any](deadline time.Time, what string, f func() (T, error)) T {
	var last error
	for time.Now().Before(deadline) {
		v, err := f()
		if err == nil {
			return v
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	fail("%s: %v", what, last)
	var zero T
	return zero
}

func fail(f string, a ...any) {
	fmt.Printf("FAILED: "+f+"\n", a...)
	os.Exit(1)
}

func main() {
	name := flag.String("name", "", "client name")
	ctrl := flag.String("ctrl", "", "control directory")
	seedList := flag.String("seeds", "", "coordinator client addresses")
	proto := flag.Int("proto", 2, "RESP protocol")
	flag.Parse()
	pause := 4 * time.Millisecond
	if p := os.Getenv("KV_MATRIX_WRITER_PAUSE_MS"); p != "" {
		if f, err := strconv.ParseFloat(p, 64); err == nil {
			pause = time.Duration(f * float64(time.Millisecond))
		}
	}
	seeds := strings.Split(*seedList, ",")
	ctx := context.Background()
	slots := func(ctx context.Context) ([]redis.ClusterSlot, error) {
		var last error
		for _, s := range seeds {
			c := redis.NewClient(&redis.Options{Addr: s, Protocol: *proto, DialTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second})
			r, err := c.ClusterSlots(ctx).Result()
			c.Close()
			if err == nil {
				return r, nil
			}
			last = err
		}
		return nil, last
	}
	opts := func(ro bool) *redis.ClusterOptions {
		return &redis.ClusterOptions{
			Addrs: seeds, ClusterSlots: slots, Protocol: *proto, MaxRedirects: 3, ReadOnly: ro,
			DialTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
		}
	}
	c := redis.NewClusterClient(opts(false))
	res := map[string]any{"name": *name, "lib": "go-redis", "ok": false}
	checks := map[string]string{}
	deadline := time.Now().Add(60 * time.Second)
	retry(deadline, "bootstrap", func() (string, error) { return c.Ping(ctx).Result() })
	checks["bootstrap"] = "ok"

	static := map[string]string{}
	var staticKeys []string
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("%s:s:%d", *name, i)
		static[k] = fmt.Sprintf("s%d-%s", i, *name)
		staticKeys = append(staticKeys, k)
	}
	for _, k := range staticKeys {
		retry(deadline, "SET "+k, func() (string, error) { return c.Set(ctx, k, static[k], 0).Result() })
	}
	for _, k := range staticKeys {
		if v := retry(deadline, "GET "+k, func() (string, error) { return c.Get(ctx, k).Result() }); v != static[k] {
			fail("GET %s: %q", k, v)
		}
	}

	// pipeline (go-redis splits it per node)
	cmds := retry(deadline, "pipeline", func() ([]redis.Cmder, error) {
		return c.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i := 0; i < 100; i++ {
				k := fmt.Sprintf("%s:p:%d", *name, i)
				p.Set(ctx, k, k, 0)
			}
			for i := 0; i < 100; i++ {
				p.Get(ctx, fmt.Sprintf("%s:p:%d", *name, i))
			}
			return nil
		})
	})
	for i := 0; i < 100; i++ {
		if v, _ := cmds[100+i].(*redis.StringCmd).Result(); v != fmt.Sprintf("%s:p:%d", *name, i) {
			fail("pipeline GET %d: %q", i, v)
		}
	}
	checks["pipeline"] = "ok (100 SET + 100 GET)"

	// MULTI/EXEC on one slot
	tag := "{" + *name + "-tx}"
	var incr *redis.IntCmd
	retry(deadline, "MULTI/EXEC", func() ([]redis.Cmder, error) {
		return c.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, tag+":a", "1", 0)
			incr = p.Incr(ctx, tag+":a")
			p.Set(ctx, tag+":b", "x", 0)
			return nil
		})
	})
	if incr.Val() != 2 {
		fail("MULTI/EXEC INCR: %d", incr.Val())
	}
	if v, _ := c.Get(ctx, tag+":b").Result(); v != "x" {
		fail("MULTI/EXEC result %q", v)
	}
	checks["multi"] = "ok (TxPipelined)"

	// CROSSSLOT: MSET of two slots, sent by go-redis to the owner of the first key
	k1, k2 := *name+":x:a", *name+":x:b"
	err := c.Do(ctx, "MSET", k1, "1", k2, "2").Err()
	if err == nil || !strings.HasPrefix(err.Error(), "CROSSSLOT") {
		fail("cross-slot MSET: %v", err)
	}
	if _, e := c.Get(ctx, k1).Result(); !errors.Is(e, redis.Nil) {
		fail("the refused MSET wrote %s", k1)
	}
	checks["crossslot"] = "ok (server CROSSSLOT)"
	b, _ := json.Marshal(checks)
	os.WriteFile(filepath.Join(*ctrl, *name+".static"), b, 0o644)
	fmt.Printf("[%s] static checks passed\n", *name)

	// verifier writer
	h := &history{keys: map[string][]attempt{}, failSince: map[int]float64{}, lastOK: map[int]float64{}}
	rnd := rand.New(rand.NewSource(int64(len(*name)) * 7919))
	stop := filepath.Join(*ctrl, "stop")
	hardEnd := time.Now().Add(900 * time.Second)
	for i := 1; time.Now().Before(hardEnd); i++ {
		if _, err := os.Stat(stop); err == nil {
			break
		}
		k := fmt.Sprintf("%s:w:%d", *name, i)
		if i > 10 && i%5 == 0 {
			k = fmt.Sprintf("%s:w:%d", *name, 1+rnd.Intn(i/5*4-1))
		}
		v := fmt.Sprintf("%s-%d", *name, i)
		t0 := ts()
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		r, err := c.Set(cctx, k, v, 0).Result()
		cancel()
		if err != nil {
			// go-redis reloads its slot map only on MOVED/ASK or when the map is 10 s old; a
			// dead primary sends no MOVED, so reload on errors (non-blocking).
			c.ReloadState(ctx)
		}
		h.record(k, v, err == nil && r == "OK", t0)
		if pause > 0 {
			time.Sleep(pause)
		}
	}
	for b, t := range h.failSince {
		h.outages = append(h.outages, [3]float64{t, ts(), float64(b)})
	}
	res["acked"], res["unacked"], res["outages"] = h.acked, h.unacked, h.outages
	fmt.Printf("[%s] writer done: %d acked, %d not acknowledged\n", *name, h.acked, h.unacked)

	// READONLY replica reads
	ro := redis.NewClusterClient(opts(true))
	deadline = time.Now().Add(60 * time.Second)
	stale := 0
	for _, k := range staticKeys {
		end := time.Now().Add(10 * time.Second)
		for {
			v := retry(deadline, "replica GET "+k, func() (string, error) { return ro.Get(ctx, k).Result() })
			if v == static[k] {
				break
			}
			if time.Now().After(end) {
				fail("replica read of %s: %q", k, v)
			}
			stale++
			time.Sleep(50 * time.Millisecond)
		}
	}
	checks["readonly"] = fmt.Sprintf("ok (%d replica reads, %d stale retries)", len(staticKeys), stale)

	// zero lost acknowledged writes
	var lost []string
	deadline = time.Now().Add(300 * time.Second)
	for _, k := range h.order {
		allowed := h.allowed(k)
		if allowed == nil {
			continue
		}
		v := retry(deadline, "verify GET "+k, func() (string, error) {
			v, err := c.Get(ctx, k).Result()
			if errors.Is(err, redis.Nil) {
				return "<nil>", nil
			}
			return v, err
		})
		if !allowed[v] {
			lost = append(lost, k+"="+v)
		}
	}
	res["lost"] = len(lost)
	if len(lost) > 5 {
		res["lost_examples"] = lost[:5]
	} else {
		res["lost_examples"] = lost
	}
	res["ok"] = len(lost) == 0
	res["checks"] = checks
	b, _ = json.Marshal(res)
	os.WriteFile(filepath.Join(*ctrl, *name+".result"), b, 0o644)
	fmt.Printf("[%s] verify: %d lost of %d acknowledged\n", *name, len(lost), h.acked)
	if len(lost) > 0 {
		os.Exit(1)
	}
}
