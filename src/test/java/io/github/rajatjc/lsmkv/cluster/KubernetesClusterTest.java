package io.github.rajatjc.lsmkv.cluster;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Runs the client against a fake Kubernetes API server that speaks the same JSON. */
class KubernetesClusterTest {
    private HttpServer api;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final CountDownLatch releaseWatch = new CountDownLatch(1);

    private static String pod(String name, String phase, boolean ready, String extraMetadata, String waitingReason) {
        String state = waitingReason == null ? "{\"running\": {}}" : "{\"waiting\": {\"reason\": \"" + waitingReason + "\"}}";
        return """
                {"metadata": {"name": "%s", "creationTimestamp": "2026-10-18T10:00:00Z"%s},
                 "spec": {"nodeName": "kind-worker"},
                 "status": {"phase": "%s", "containerStatuses": [{"ready": %s, "restartCount": 1, "state": %s}]}}
                """.formatted(name, extraMetadata, phase, ready, state).replace("\n", "");
    }

    @BeforeEach
    void startFakeApi() throws IOException {
        api = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        api.createContext("/api/v1/namespaces/demo/pods", this::handle);
        api.start();
    }

    @AfterEach
    void stop() {
        releaseWatch.countDown();
        api.stop(0);
    }

    private void handle(HttpExchange ex) throws IOException {
        requests.add(ex.getRequestMethod() + " " + ex.getRequestURI() + " auth=" + ex.getRequestHeaders().getFirst("Authorization"));
        String query = String.valueOf(ex.getRequestURI().getQuery());
        if (ex.getRequestMethod().equals("DELETE")) {
            respond(ex, 200, "{}");
        } else if (query.contains("watch=true")) {
            // A pod killed and recreated by its StatefulSet, as watch events.
            ex.sendResponseHeaders(200, 0);
            try (OutputStream out = ex.getResponseBody()) {
                for (String event : List.of(
                        "{\"type\": \"MODIFIED\", \"object\": " + pod("lsm-kv-1", "Running", false, ", \"deletionTimestamp\": \"2026-10-18T10:05:00Z\"", null) + "}",
                        "{\"type\": \"DELETED\", \"object\": " + pod("lsm-kv-1", "Running", false, "", null) + "}",
                        "{\"type\": \"ADDED\", \"object\": " + pod("lsm-kv-1", "Pending", false, "", "ContainerCreating") + "}")) {
                    out.write((event + "\n").getBytes(UTF_8));
                    out.flush();
                }
                releaseWatch.await(5, TimeUnit.SECONDS); // hold the stream open like a real watch
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            respond(ex, 200, "{\"metadata\": {\"resourceVersion\": \"42\"}, \"items\": ["
                    + pod("lsm-kv-0", "Running", true, "", null) + ","
                    + pod("lsm-kv-1", "Running", true, "", null) + "]}");
        }
    }

    private static void respond(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(UTF_8);
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Test
    void listsWatchesAndKills() throws Exception {
        KubernetesCluster cluster = new KubernetesCluster("http://localhost:" + api.getAddress().getPort(), "demo",
                "app=lsm-kv", HttpClient.newHttpClient(), () -> "test-token");

        cluster.start();
        PodFeed.Snapshot listed = awaitPods(cluster, pods -> pods.size() == 2 && pods.get(1).ready());
        PodView p = listed.pods().get(0);
        assertEquals("lsm-kv-0", p.name());
        assertEquals("Running", p.status());
        assertEquals(1, p.restarts());
        assertEquals("kind-worker", p.node());

        cluster.kill("lsm-kv-0");
        assertTrue(requests.contains("DELETE /api/v1/namespaces/demo/pods/lsm-kv-0 auth=Bearer test-token"), requests.toString());
        assertThrows(IllegalArgumentException.class, () -> cluster.kill("not-ours"));

        // The watch replays lsm-kv-1 terminating, disappearing and coming back.
        PodFeed.Snapshot recreated = awaitPods(cluster, pods -> pods.size() == 2 && pods.get(1).status().equals("ContainerCreating"));
        assertFalse(recreated.pods().get(1).ready());
        assertTrue(requests.stream().anyMatch(r -> r.contains("watch=true") && r.contains("resourceVersion=42")
                && r.contains("labelSelector=app%3Dlsm-kv")), requests.toString());
    }

    @Test
    void derivesKubectlStyleStatus() {
        assertEquals("Terminating", KubernetesCluster.toView(JsonReader.parse(
                pod("p", "Running", true, ", \"deletionTimestamp\": \"2026-10-18T10:05:00Z\"", null))).status());
        assertEquals("CrashLoopBackOff", KubernetesCluster.toView(JsonReader.parse(
                pod("p", "Running", false, "", "CrashLoopBackOff"))).status());
        PodView pending = KubernetesCluster.toView(JsonReader.parse(
                "{\"metadata\": {\"name\": \"p\"}, \"status\": {\"phase\": \"Pending\"}}"));
        assertEquals("Pending", pending.status());
        assertFalse(pending.ready());
    }

    private static PodFeed.Snapshot awaitPods(KubernetesCluster cluster, Predicate<List<PodView>> condition) throws InterruptedException {
        PodFeed.Snapshot s = cluster.feed().current();
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.test(s.pods())) {
            if (System.nanoTime() > deadline) {
                fail("timed out; last snapshot: " + s);
            }
            s = cluster.feed().awaitNewer(s.version(), Duration.ofMillis(200));
        }
        return s;
    }
}
