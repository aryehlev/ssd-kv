// Jedis and Lettuce verifier clients of the cluster client matrix (DESIGN §25.6).
//
// Usage: java -cp <classpath> MatrixClient --name jedis|lettuce2|lettuce3 --ctrl DIR --seeds h:p,...
//
// Settings per DESIGN §23.5. Jedis: JedisCluster over all coordinators (none beyond that).
// Lettuce: ClusterTopologyRefreshOptions with dynamicRefreshSources(false), all adaptive refresh
// triggers and a 30 s periodic refresh; RESP2 or RESP3.
//
// Phases: static checks (bootstrap, pipeline, MULTI on one slot, server CROSSSLOT) -> NAME.static;
// a verifier writer until DIR/stop; READONLY replica reads (Lettuce: ReadFrom.REPLICA; Jedis has
// no replica routing in JedisCluster, so it reads at the replicas CLUSTER SLOTS lists, after
// READONLY); verification of every acknowledged write -> NAME.result (JSON). Every wait is
// bounded (socket and command timeouts, deadlines).

import io.lettuce.core.ClientOptions;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.protocol.ProtocolVersion;
import redis.clients.jedis.ClusterPipeline;
import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class MatrixClient {
    /** The operations a verifier needs from either library. */
    interface Kv {
        String set(String k, String v);
        String get(String k);
        String pipeline(String name);
        String multi(String name);
        String crossSlot(String k1, String k2);
        Kv replicaReader();
        void close();
    }

    static double ts() { return System.currentTimeMillis() / 1000.0; }

    static <T> T retry(long deadline, String what, Supplier<T> f) {
        RuntimeException last = null;
        while (System.currentTimeMillis() < deadline) {
            try { return f.get(); } catch (RuntimeException e) { last = e; }
            try { Thread.sleep(100); } catch (InterruptedException ie) { throw new RuntimeException(ie); }
        }
        throw new RuntimeException(what + ": " + last, last);
    }

    static void check(boolean ok, String what) { if (!ok) throw new RuntimeException(what); }

    // ---------------------------------------------------------------------------- Jedis

    static final class JedisKv implements Kv {
        final JedisCluster jc;
        final List<HostAndPort> seeds;
        JedisKv(List<HostAndPort> seeds) {
            this.seeds = seeds;
            // 1 s socket timeouts and a 1 s bound on one call's retries (Jedis's default bound,
            // soTimeout x maxAttempts, kept one SET blocked for 13 s across a failover: its
            // backoff sleeps scale with the time left and it renews its slot map late)
            jc = new JedisCluster(new HashSet<>(seeds),
                redis.clients.jedis.DefaultJedisClientConfig.builder().connectionTimeoutMillis(1000).socketTimeoutMillis(1000).build(),
                5, Duration.ofMillis(1000), new ConnectionPoolConfig());
        }
        public String set(String k, String v) { return jc.set(k, v); }
        public String get(String k) { return jc.get(k); }
        public String pipeline(String name) {
            ClusterPipeline p = jc.pipelined();
            List<Response<String>> sets = new ArrayList<>(), gets = new ArrayList<>();
            for (int i = 0; i < 100; i++) sets.add(p.set(name + ":p:" + i, "v" + i));
            for (int i = 0; i < 100; i++) gets.add(p.get(name + ":p:" + i));
            p.sync();
            p.close();
            for (int i = 0; i < 100; i++) {
                check("OK".equals(sets.get(i).get()), "pipeline SET " + i);
                check(("v" + i).equals(gets.get(i).get()), "pipeline GET " + i);
            }
            return "ok (ClusterPipeline, 100 SET + 100 GET)";
        }
        public String multi(String name) {
            String tag = "{" + name + "-tx}";
            String mode;
            List<Object> r;
            try {
                Transaction t = jc.multi();
                t.set(tag + ":a", "1"); t.incr(tag + ":a"); t.set(tag + ":b", "x"); t.get(tag + ":a");
                r = t.exec();
                t.close();
                mode = "JedisCluster.multi";
            } catch (UnsupportedOperationException e) {
                try (Connection c = jc.getConnectionFromSlot(JedisClusterCRC16.getSlot(tag + ":a"))) {
                    Transaction t = new Transaction(c);
                    t.set(tag + ":a", "1"); t.incr(tag + ":a"); t.set(tag + ":b", "x"); t.get(tag + ":a");
                    r = t.exec();
                }
                mode = "slot connection transaction";
            }
            check(r != null && r.size() == 4 && "OK".equals(r.get(0)) && Long.valueOf(2).equals(r.get(1))
                && "2".equals(r.get(3)), "MULTI/EXEC replies " + r);
            check("x".equals(jc.get(tag + ":b")), "MULTI/EXEC wrote b");
            return "ok (" + mode + ")";
        }
        public String crossSlot(String k1, String k2) {
            try (Connection c = jc.getConnectionFromSlot(JedisClusterCRC16.getSlot(k1))) {
                new Jedis(c).mset(k1, "1", k2, "2");
            } catch (redis.clients.jedis.exceptions.JedisDataException e) {
                return e.getMessage();
            }
            return null;
        }
        public Kv replicaReader() { return new JedisReplicaKv(seeds); }
        public void close() { jc.close(); }
    }

    /** READONLY reads at the replicas the coordinators list in CLUSTER SLOTS. */
    static final class JedisReplicaKv implements Kv {
        final Map<Integer, List<HostAndPort>> replicas = new HashMap<>();
        final Map<HostAndPort, Jedis> conns = new HashMap<>();
        JedisReplicaKv(List<HostAndPort> seeds) {
            RuntimeException last = null;
            for (HostAndPort s : seeds) {
                try (Jedis j = new Jedis(s.getHost(), s.getPort(), 2000)) {
                    for (Object range : j.clusterSlots()) {
                        List<?> r = (List<?>) range;
                        int a = ((Long) r.get(0)).intValue(), b = ((Long) r.get(1)).intValue();
                        List<HostAndPort> reps = new ArrayList<>();
                        for (int i = 3; i < r.size(); i++) {
                            List<?> n = (List<?>) r.get(i);
                            reps.add(new HostAndPort(new String((byte[]) n.get(0)), ((Long) n.get(1)).intValue()));
                        }
                        for (int s2 = a; s2 <= b; s2++) replicas.put(s2, reps);
                    }
                    return;
                } catch (RuntimeException e) { last = e; }
            }
            throw new RuntimeException("CLUSTER SLOTS: " + last);
        }
        public String get(String k) {
            List<HostAndPort> reps = replicas.get(JedisClusterCRC16.getSlot(k));
            check(reps != null && !reps.isEmpty(), "no replica listed for " + k);
            HostAndPort h = reps.get(0);
            Jedis j = conns.computeIfAbsent(h, x -> { Jedis n = new Jedis(x.getHost(), x.getPort(), 3000); n.readonly(); return n; });
            try { return j.get(k); } catch (RuntimeException e) { conns.remove(h); j.close(); throw e; }
        }
        public String set(String k, String v) { throw new UnsupportedOperationException(); }
        public String pipeline(String n) { throw new UnsupportedOperationException(); }
        public String multi(String n) { throw new UnsupportedOperationException(); }
        public String crossSlot(String a, String b) { throw new UnsupportedOperationException(); }
        public Kv replicaReader() { return this; }
        public void close() { conns.values().forEach(Jedis::close); }
    }

    // ---------------------------------------------------------------------------- Lettuce

    static final class LettuceKv implements Kv {
        final RedisClusterClient client;
        final StatefulRedisClusterConnection<String, String> conn;
        final RedisAdvancedClusterCommands<String, String> s;
        final List<RedisURI> uris;
        final boolean resp3;
        LettuceKv(List<RedisURI> uris, boolean resp3, boolean replicas) {
            this.uris = uris;
            this.resp3 = resp3;
            client = RedisClusterClient.create(uris);
            // DESIGN §23.5 settings, plus a short adaptive-refresh rate limit and a refresh on the
            // first failed reconnect: with Lettuce's defaults (30 s, 5 reconnects) a refresh
            // taken before the promotion commits blocks the next one for 30 s
            ClusterTopologyRefreshOptions topo = ClusterTopologyRefreshOptions.builder().dynamicRefreshSources(false)
                .enableAllAdaptiveRefreshTriggers().enablePeriodicRefresh(Duration.ofSeconds(30))
                .adaptiveRefreshTriggersTimeout(Duration.ofMillis(500)).refreshTriggersReconnectAttempts(1).build();
            client.setOptions(ClusterClientOptions.builder().topologyRefreshOptions(topo)
                .protocolVersion(resp3 ? ProtocolVersion.RESP3 : ProtocolVersion.RESP2)
                .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofSeconds(2)).build())
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(1)))
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .build());
            client.setDefaultTimeout(Duration.ofSeconds(1));
            conn = client.connect();
            if (replicas) conn.setReadFrom(ReadFrom.REPLICA);
            s = conn.sync();
        }
        public String set(String k, String v) { return s.set(k, v); }
        public String get(String k) { return s.get(k); }
        public String pipeline(String name) {
            RedisAdvancedClusterAsyncCommands<String, String> a = conn.async();
            conn.setAutoFlushCommands(false);
            List<RedisFuture<String>> sets = new ArrayList<>(), gets = new ArrayList<>();
            try {
                for (int i = 0; i < 100; i++) sets.add(a.set(name + ":p:" + i, "v" + i));
                for (int i = 0; i < 100; i++) gets.add(a.get(name + ":p:" + i));
                conn.flushCommands();
                List<RedisFuture<String>> all = new ArrayList<>(sets);
                all.addAll(gets);
                check(LettuceFutures.awaitAll(10, TimeUnit.SECONDS, all.toArray(new RedisFuture[0])), "pipeline timed out");
                for (int i = 0; i < 100; i++) {
                    check("OK".equals(sets.get(i).get()), "pipeline SET " + i);
                    check(("v" + i).equals(gets.get(i).get()), "pipeline GET " + i);
                }
            } catch (Exception e) {
                throw new RuntimeException("pipeline: " + e, e);
            } finally {
                conn.setAutoFlushCommands(true);
            }
            return "ok (manual flush, 100 SET + 100 GET)";
        }
        StatefulRedisConnection<String, String> nodeFor(String key) {
            RedisClusterNode n = conn.getPartitions().getPartitionBySlot(SlotHash.getSlot(key));
            check(n != null, "no owner known for " + key);
            return conn.getConnection(n.getUri().getHost(), n.getUri().getPort());
        }
        public String multi(String name) {
            String tag = "{" + name + "-tx}";
            RedisCommands<String, String> n = nodeFor(tag + ":a").sync();
            n.multi();
            n.set(tag + ":a", "1"); n.incr(tag + ":a"); n.set(tag + ":b", "x"); n.get(tag + ":a");
            TransactionResult r = n.exec();
            check(!r.wasDiscarded() && r.size() == 4 && "OK".equals(r.get(0)) && Long.valueOf(2).equals(r.get(1))
                && "2".equals(r.get(3)), "MULTI/EXEC replies " + r.stream().toList());
            check("x".equals(s.get(tag + ":b")), "MULTI/EXEC wrote b");
            return "ok (node connection of the slot owner)";
        }
        public String crossSlot(String k1, String k2) {
            Map<String, String> m = new LinkedHashMap<>();
            m.put(k1, "1");
            m.put(k2, "2");
            try {
                nodeFor(k1).sync().mset(m);
            } catch (io.lettuce.core.RedisCommandExecutionException e) {
                return e.getMessage();
            }
            return null;
        }
        public Kv replicaReader() { return new LettuceKv(uris, resp3, true); }
        public void close() { conn.close(); client.shutdown(Duration.ZERO, Duration.ofSeconds(2)); }
    }

    // ---------------------------------------------------------------------------- verifier

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < argv.length; i += 2) a.put(argv[i].replaceFirst("^--", ""), argv[i + 1]);
        String name = a.get("name");
        Path ctrl = Path.of(a.get("ctrl"));
        List<HostAndPort> seeds = new ArrayList<>();
        List<RedisURI> uris = new ArrayList<>();
        for (String s : a.get("seeds").split(",")) {
            String[] hp = s.split(":");
            seeds.add(new HostAndPort(hp[0], Integer.parseInt(hp[1])));
            uris.add(RedisURI.Builder.redis(hp[0], Integer.parseInt(hp[1])).withTimeout(Duration.ofSeconds(3)).build());
        }
        double pauseMs = Double.parseDouble(System.getenv().getOrDefault("KV_MATRIX_WRITER_PAUSE_MS", "4"));
        Map<String, Object> res = new LinkedHashMap<>();
        Map<String, String> checks = new LinkedHashMap<>();
        res.put("name", name);
        res.put("lib", name.startsWith("jedis") ? "jedis" : "lettuce");
        long deadline = System.currentTimeMillis() + 60_000;
        Kv kv = retry(deadline, "bootstrap", () -> name.startsWith("jedis")
            ? new JedisKv(seeds) : new LettuceKv(uris, name.endsWith("3"), false));
        checks.put("bootstrap", "ok");

        Map<String, String> statics = new LinkedHashMap<>();
        for (int i = 0; i < 200; i++) statics.put(name + ":s:" + i, "s" + i + "-" + name);
        for (var e : statics.entrySet()) retry(deadline, "SET " + e.getKey(), () -> kv.set(e.getKey(), e.getValue()));
        for (var e : statics.entrySet()) {
            String got = retry(deadline, "GET " + e.getKey(), () -> kv.get(e.getKey()));
            check(e.getValue().equals(got), "GET " + e.getKey() + ": " + got);
        }
        checks.put("pipeline", retry(deadline, "pipeline", () -> kv.pipeline(name)));
        checks.put("multi", retry(deadline, "MULTI/EXEC", () -> kv.multi(name)));
        String k1 = name + ":x:a", k2 = name + ":x:b";
        while (JedisClusterCRC16.getSlot(k1) == JedisClusterCRC16.getSlot(k2)) k2 += "b";
        final String k2f = k2;
        String cross = retry(deadline, "cross-slot MSET", () -> kv.crossSlot(k1, k2f));
        check(cross != null && cross.startsWith("CROSSSLOT"), "cross-slot MSET: " + cross);
        check(kv.get(k1) == null, "the refused MSET wrote a key");
        checks.put("crossslot", "ok (server CROSSSLOT)");
        Files.writeString(ctrl.resolve(name + ".static"), checks.toString());
        System.out.println("[" + name + "] static checks passed");

        // verifier writer
        Map<String, List<Object[]>> hist = new LinkedHashMap<>();
        // client-visible outages per slot range (slot >> 12: four ranges aligned with every
        // topology boundary of the matrix), so writes to healthy shards never mask an outage
        List<double[]> outages = new ArrayList<>();
        int acked = 0, unacked = 0;
        double[] failSince = {-1, -1, -1, -1}, lastOk = {-1, -1, -1, -1};
        Random rnd = new Random(name.hashCode());
        Path stop = ctrl.resolve("stop");
        long hardEnd = System.currentTimeMillis() + 900_000;
        for (int i = 1; !Files.exists(stop) && System.currentTimeMillis() < hardEnd; i++) {
            String k = (i > 10 && i % 5 == 0) ? name + ":w:" + (1 + rnd.nextInt(i / 5 * 4 - 1)) : name + ":w:" + i;
            String v = name + "-" + i;
            boolean ok;
            double t0 = ts();
            try { ok = "OK".equals(kv.set(k, v)); } catch (RuntimeException e) { ok = false; }
            hist.computeIfAbsent(k, x -> new ArrayList<>()).add(new Object[]{v, ok});
            double now = ts();
            int b = JedisClusterCRC16.getSlot(k) >> 12;
            // a gap between two successful writes of one range longer than 250 ms is an outage
            // (failed writes in between, or one call that blocked while it retried)
            double last = lastOk[b] >= 0 ? lastOk[b] : t0;
            if (ok) {
                acked++;
                if (now - last > 0.25 || failSince[b] >= 0) outages.add(new double[]{last, now, b});
                failSince[b] = -1;
                lastOk[b] = now;
            } else {
                unacked++;
                if (failSince[b] < 0) failSince[b] = last;
            }
            if (pauseMs > 0) Thread.sleep((long) pauseMs);
        }
        for (int b = 0; b < 4; b++) if (failSince[b] >= 0) outages.add(new double[]{failSince[b], ts(), b});
        System.out.println("[" + name + "] writer done: " + acked + " acked, " + unacked + " not acknowledged");

        // READONLY replica reads
        deadline = System.currentTimeMillis() + 60_000;
        Kv ro = retry(deadline, "replica reader", kv::replicaReader);
        int stale = 0;
        for (var e : statics.entrySet()) {
            long end = System.currentTimeMillis() + 10_000;
            while (true) {
                String got = retry(deadline, "replica GET " + e.getKey(), () -> ro.get(e.getKey()));
                if (e.getValue().equals(got)) break;
                check(System.currentTimeMillis() < end, "replica read of " + e.getKey() + ": " + got);
                stale++;
                Thread.sleep(50);
            }
        }
        checks.put("readonly", "ok (" + statics.size() + " replica reads, " + stale + " stale retries)");

        // zero lost acknowledged writes: the last acknowledged value or any unacknowledged one
        List<String> lost = new ArrayList<>();
        deadline = System.currentTimeMillis() + 300_000;
        for (var e : hist.entrySet()) {
            List<Object[]> h = e.getValue();
            int last = -1;
            for (int i = 0; i < h.size(); i++) if ((Boolean) h.get(i)[1]) last = i;
            if (last < 0) continue;
            Set<String> allowed = new HashSet<>();
            allowed.add((String) h.get(last)[0]);
            for (Object[] x : h) if (!(Boolean) x[1]) allowed.add((String) x[0]);
            String got = retry(deadline, "verify GET " + e.getKey(), () -> kv.get(e.getKey()));
            if (!allowed.contains(got)) lost.add(e.getKey() + "=" + got);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{\"name\":\"").append(name).append("\",\"lib\":\"").append(res.get("lib"))
            .append("\",\"ok\":").append(lost.isEmpty()).append(",\"acked\":").append(acked)
            .append(",\"unacked\":").append(unacked).append(",\"lost\":").append(lost.size())
            .append(",\"lost_examples\":[");
        for (int i = 0; i < Math.min(5, lost.size()); i++) sb.append(i > 0 ? "," : "").append('"').append(lost.get(i).replace("\"", "'")).append('"');
        sb.append("],\"outages\":[");
        for (int i = 0; i < outages.size(); i++) sb.append(i > 0 ? "," : "").append('[').append(outages.get(i)[0]).append(',').append(outages.get(i)[1]).append(',').append((int) outages.get(i)[2]).append(']');
        sb.append("],\"checks\":{");
        int j = 0;
        for (var e : checks.entrySet()) sb.append(j++ > 0 ? "," : "").append('"').append(e.getKey()).append("\":\"").append(e.getValue().replace("\"", "'")).append('"');
        sb.append("}}");
        Files.writeString(ctrl.resolve(name + ".result"), sb.toString());
        System.out.println("[" + name + "] verify: " + lost.size() + " lost of " + acked + " acknowledged");
        ro.close();
        kv.close();
        System.exit(lost.isEmpty() ? 0 : 1);
    }
}
