package io.github.rajatjc.lsmkv.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.rajatjc.lsmkv.cluster.Cluster;
import io.github.rajatjc.lsmkv.cluster.KubernetesCluster;
import io.github.rajatjc.lsmkv.cluster.PodFeed;
import io.github.rajatjc.lsmkv.cluster.PodView;
import io.github.rajatjc.lsmkv.cluster.SimulatedCluster;
import io.github.rajatjc.lsmkv.storage.LsmStore;
import io.github.rajatjc.lsmkv.storage.Options;
import io.github.rajatjc.lsmkv.storage.Stats;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * HTTP API for the store, plus the web UI. Uses the JDK's built-in server with
 * one virtual thread per request, so it has no runtime dependencies.
 *
 * <p>Environment: PORT (8080), DATA_DIR (./data), SYNC_WRITES ("true" to fsync
 * every write), CORS_ORIGIN (when the UI is hosted elsewhere), MAX_DATA_MB (100),
 * CLUSTER_MODE (demo, kubernetes or off; defaults to kubernetes inside a cluster
 * and demo elsewhere), POD_SELECTOR (app=lsm-kv).
 */
public final class KvServer {
    private static final int MAX_KEY_BYTES = 256;
    private static final int MAX_VALUE_BYTES = 64 << 10;
    private static final int MAX_BULK = 5000;
    private static final String KV_PATH = "/api/kv/";
    private static final Pattern KILL_PATH = Pattern.compile("/api/cluster/pods/([^/]+)/kill");
    private static final long KILL_INTERVAL_MS = 2000;

    private final LsmStore store;
    private final long maxDataBytes;
    private final String corsOrigin;
    private final Cluster cluster; // null when the cluster view is off
    private final AtomicLong lastKill = new AtomicLong();
    private final byte[] indexHtml;

    KvServer(LsmStore store, long maxDataBytes, String corsOrigin, Cluster cluster) throws IOException {
        this.store = store;
        this.maxDataBytes = maxDataBytes;
        this.corsOrigin = corsOrigin;
        this.cluster = cluster;
        try (InputStream in = KvServer.class.getResourceAsStream("/public/index.html")) {
            if (in == null) {
                throw new IOException("missing /public/index.html on the classpath");
            }
            this.indexHtml = in.readAllBytes();
        }
    }

    public static void main(String[] args) throws IOException, GeneralSecurityException {
        LsmStore store = LsmStore.open(new Options(
                Path.of(env("DATA_DIR", "./data")),
                64 << 10, // small, so flushes and compactions are visible in the demo
                4,
                env("SYNC_WRITES", "false").equals("true")));
        KvServer app = new KvServer(store, Long.parseLong(env("MAX_DATA_MB", "100")) << 20, env("CORS_ORIGIN", ""),
                cluster(env("CLUSTER_MODE", System.getenv("KUBERNETES_SERVICE_HOST") != null ? "kubernetes" : "demo")));

        HttpServer http = HttpServer.create(new InetSocketAddress(Integer.parseInt(env("PORT", "8080"))), 0);
        http.createContext("/", app::handle);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        // Hosting platforms send SIGTERM on redeploy; close the store so the WAL is synced.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            http.stop(1);
            try {
                store.close();
            } catch (IOException e) {
                System.err.println("close: " + e);
            }
        }));
        http.start();
        System.out.println("listening on :" + http.getAddress().getPort());
    }

    private static Cluster cluster(String mode) throws IOException, GeneralSecurityException {
        System.out.println("cluster view: " + mode);
        return switch (mode) {
            case "demo" -> new SimulatedCluster(3, Duration.ofSeconds(1));
            case "kubernetes" -> KubernetesCluster.inCluster(env("POD_SELECTOR", "app=lsm-kv"));
            case "off" -> null;
            default -> throw new IllegalArgumentException("CLUSTER_MODE must be demo, kubernetes or off, not " + mode);
        };
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? fallback : v;
    }

    private void handle(HttpExchange ex) {
        try {
            if (!corsOrigin.isEmpty()) {
                var headers = ex.getResponseHeaders();
                headers.set("Access-Control-Allow-Origin", corsOrigin);
                headers.set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST, OPTIONS");
                headers.set("Access-Control-Allow-Headers", "Content-Type");
                if (ex.getRequestMethod().equals("OPTIONS")) {
                    send(ex, 204, null, null);
                    return;
                }
            }
            route(ex, ex.getRequestMethod(), ex.getRequestURI().getPath());
        } catch (IllegalArgumentException e) {
            tryError(ex, 400, e.getMessage());
        } catch (Exception e) {
            tryError(ex, 500, e.toString());
        } finally {
            ex.close();
        }
    }

    private void route(HttpExchange ex, String method, String path) throws IOException {
        if (path.startsWith(KV_PATH)) {
            kv(ex, method, path.substring(KV_PATH.length()));
            return;
        }
        if (path.startsWith("/api/cluster")) {
            cluster(ex, method, path);
            return;
        }
        switch (method + " " + path) {
            case "GET /", "GET /index.html" -> send(ex, 200, "text/html; charset=utf-8", indexHtml);
            case "GET /healthz" -> send(ex, 200, "text/plain", "ok".getBytes(UTF_8));
            case "GET /api/stats" -> json(ex, 200, statsJson(store.stats()));
            case "GET /api/scan" -> scan(ex);
            case "POST /api/flush" -> {
                store.flush();
                json(ex, 200, statsJson(store.stats()));
            }
            case "POST /api/compact" -> {
                store.compact();
                json(ex, 200, statsJson(store.stats()));
            }
            case "POST /api/bulk" -> bulk(ex);
            default -> error(ex, 404, "not found");
        }
    }

    private void kv(HttpExchange ex, String method, String key) throws IOException {
        byte[] k = key.getBytes(UTF_8);
        if (k.length == 0 || k.length > MAX_KEY_BYTES) {
            error(ex, 400, "key must be 1-256 bytes");
            return;
        }
        switch (method) {
            case "GET" -> {
                Optional<byte[]> v = store.get(k);
                if (v.isEmpty()) {
                    error(ex, 404, "not found");
                } else {
                    json(ex, 200, Json.object("key", key, "value", new String(v.get(), UTF_8)));
                }
            }
            case "PUT" -> {
                if (full()) {
                    error(ex, 507, "demo storage limit reached");
                    return;
                }
                byte[] body = ex.getRequestBody().readNBytes(MAX_VALUE_BYTES + 1);
                if (body.length > MAX_VALUE_BYTES) {
                    error(ex, 413, "value must be at most 64KB");
                    return;
                }
                store.put(k, body);
                send(ex, 204, null, null);
            }
            case "DELETE" -> {
                store.delete(k);
                send(ex, 204, null, null);
            }
            default -> error(ex, 405, "method not allowed");
        }
    }

    private void scan(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex);
        int limit = parseInt(q.get("limit"), 100);
        if (limit <= 0 || limit > 1000) {
            limit = 100;
        }
        List<String> rows = store.scan(q.getOrDefault("prefix", "").getBytes(UTF_8), limit).stream()
                .map(e -> Json.object("key", new String(e.key(), UTF_8), "value", new String(e.value(), UTF_8)))
                .toList();
        json(ex, 200, Json.array(rows));
    }

    /** Writes n random keys so the UI can show flushes and compactions happen. */
    private void bulk(HttpExchange ex) throws IOException {
        int n = parseInt(query(ex).get("n"), 1000);
        if (n <= 0 || n > MAX_BULK) {
            n = 1000;
        }
        if (full()) {
            error(ex, 507, "demo storage limit reached");
            return;
        }
        long start = System.nanoTime();
        HexFormat hex = HexFormat.of();
        byte[] id = new byte[8];
        for (int i = 0; i < n; i++) {
            ThreadLocalRandom.current().nextBytes(id);
            String h = hex.formatHex(id);
            store.put(("user:" + h).getBytes(UTF_8), ("{\"id\":\"" + h + "\"}").getBytes(UTF_8));
        }
        json(ex, 200, Json.object("written", n, "elapsedMs", (System.nanoTime() - start) / 1_000_000));
    }

    private void cluster(HttpExchange ex, String method, String path) throws IOException {
        if (cluster == null) {
            error(ex, 404, "the cluster view is off");
            return;
        }
        Matcher kill = KILL_PATH.matcher(path);
        if (method.equals("GET") && path.equals("/api/cluster")) {
            json(ex, 200, clusterJson(cluster.feed().current()));
        } else if (method.equals("GET") && path.equals("/api/cluster/events")) {
            streamCluster(ex);
        } else if (method.equals("POST") && kill.matches()) {
            long now = System.currentTimeMillis();
            long last = lastKill.get();
            if (now - last < KILL_INTERVAL_MS || !lastKill.compareAndSet(last, now)) {
                error(ex, 429, "one kill every 2 seconds, please");
                return;
            }
            try {
                cluster.kill(kill.group(1));
            } catch (IllegalStateException e) {
                error(ex, 409, e.getMessage());
                return;
            }
            send(ex, 202, null, null);
        } else {
            error(ex, 404, "not found");
        }
    }

    /** Server-Sent Events: the pod list now, then again after every change. */
    private void streamCluster(HttpExchange ex) throws IOException {
        var headers = ex.getResponseHeaders();
        headers.set("Content-Type", "text/event-stream");
        headers.set("Cache-Control", "no-cache");
        headers.set("X-Accel-Buffering", "no"); // tell proxies not to buffer the stream
        ex.sendResponseHeaders(200, 0);
        OutputStream out = ex.getResponseBody();
        PodFeed.Snapshot snapshot = cluster.feed().current();
        try {
            while (true) {
                out.write(("data: " + clusterJson(snapshot) + "\n\n").getBytes(UTF_8));
                out.flush();
                PodFeed.Snapshot next = cluster.feed().awaitNewer(snapshot.version(), Duration.ofSeconds(15));
                while (next.version() == snapshot.version()) {
                    out.write(": keep-alive\n\n".getBytes(UTF_8)); // also detects closed tabs
                    out.flush();
                    next = cluster.feed().awaitNewer(snapshot.version(), Duration.ofSeconds(15));
                }
                snapshot = next;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException gone) {
            // the browser closed the stream
        }
    }

    private String clusterJson(PodFeed.Snapshot snapshot) {
        List<String> pods = snapshot.pods().stream().map(KvServer::podJson).toList();
        return Json.object(
                "mode", cluster.mode(),
                "namespace", cluster.namespace(),
                "pods", new Json.Raw(Json.array(pods)),
                "error", snapshot.error() == null ? new Json.Raw("null") : snapshot.error());
    }

    private static String podJson(PodView p) {
        return Json.object(
                "name", p.name(),
                "status", p.status(),
                "ready", p.ready(),
                "restarts", p.restarts(),
                "createdAt", p.createdAt(),
                "node", p.node() == null ? new Json.Raw("null") : p.node(),
                "killable", p.killable());
    }

    private boolean full() {
        return store.stats().dataBytes() > maxDataBytes;
    }

    private static String statsJson(Stats s) {
        List<String> tables = s.tables().stream()
                .map(t -> Json.object("id", t.id(), "entries", t.entries(), "bytes", t.bytes()))
                .toList();
        return Json.object(
                "memtableEntries", s.memtableEntries(),
                "memtableBytes", s.memtableBytes(),
                "memtableLimit", s.memtableLimit(),
                "walBytes", s.walBytes(),
                "tables", new Json.Raw(Json.array(tables)),
                "compactAt", s.compactAt(),
                "flushes", s.flushes(),
                "compactions", s.compactions(),
                "bloomSkips", s.bloomSkips(),
                "tableLookups", s.tableLookups());
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> out = new HashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw == null) {
            return out;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String k = eq < 0 ? pair : pair.substring(0, eq);
            String v = eq < 0 ? "" : pair.substring(eq + 1);
            out.put(URLDecoder.decode(k, UTF_8), URLDecoder.decode(v, UTF_8));
        }
        return out;
    }

    private static int parseInt(String s, int fallback) {
        try {
            return s == null ? fallback : Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static void json(HttpExchange ex, int status, String body) throws IOException {
        send(ex, status, "application/json", body.getBytes(UTF_8));
    }

    private static void error(HttpExchange ex, int status, String message) throws IOException {
        json(ex, status, Json.object("error", message));
    }

    /** Best effort: fails quietly if a response was already started. */
    private static void tryError(HttpExchange ex, int status, String message) {
        try {
            error(ex, status, message);
        } catch (IOException | RuntimeException ignored) {
            // the client gets a truncated response
        }
    }

    private static void send(HttpExchange ex, int status, String contentType, byte[] body) throws IOException {
        if (contentType != null) {
            ex.getResponseHeaders().set("Content-Type", contentType);
        }
        if (body == null) {
            ex.sendResponseHeaders(status, -1); // no body
            return;
        }
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(body);
        }
    }
}
