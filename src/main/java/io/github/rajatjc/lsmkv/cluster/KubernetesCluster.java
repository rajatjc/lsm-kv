package io.github.rajatjc.lsmkv.cluster;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Real mode: watches pods through the Kubernetes API and deletes them on
 * request. Talks to the API server directly over HTTPS with the pod's service
 * account, so it needs no client library.
 *
 * <p>The loop lists the pods, then follows a watch from that list's
 * resourceVersion. When the watch ends (timeout, error, 410 Gone) it lists again.
 */
public final class KubernetesCluster implements Cluster {
    private static final Path SERVICE_ACCOUNT = Path.of("/var/run/secrets/kubernetes.io/serviceaccount");

    private final String api;
    private final String namespace;
    private final String selector;
    private final HttpClient http;
    private final Supplier<String> token;
    private final Map<String, PodView> pods = new ConcurrentHashMap<>(); // written only by the watch thread
    private final PodFeed feed = new PodFeed();

    KubernetesCluster(String api, String namespace, String selector, HttpClient http, Supplier<String> token) {
        this.api = api;
        this.namespace = namespace;
        this.selector = selector;
        this.http = http;
        this.token = token;
    }

    /** Connects with the service account Kubernetes mounts into every pod, and starts watching. */
    public static KubernetesCluster inCluster(String selector) throws IOException, GeneralSecurityException {
        String host = System.getenv("KUBERNETES_SERVICE_HOST");
        String port = System.getenv("KUBERNETES_SERVICE_PORT");
        if (host == null || port == null) {
            throw new IllegalStateException("not running inside Kubernetes (KUBERNETES_SERVICE_HOST is unset)");
        }
        String namespace = Files.readString(SERVICE_ACCOUNT.resolve("namespace")).trim();
        HttpClient http = HttpClient.newBuilder()
                .sslContext(trusting(SERVICE_ACCOUNT.resolve("ca.crt")))
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        // Projected tokens rotate, so read the file on every request.
        Supplier<String> token = () -> {
            try {
                return Files.readString(SERVICE_ACCOUNT.resolve("token")).trim();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        String hostPart = host.contains(":") ? "[" + host + "]" : host; // IPv6
        KubernetesCluster cluster = new KubernetesCluster("https://" + hostPart + ":" + port, namespace, selector, http, token);
        cluster.start();
        return cluster;
    }

    private static SSLContext trusting(Path caFile) throws IOException, GeneralSecurityException {
        KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
        trust.load(null, null);
        try (InputStream in = Files.newInputStream(caFile)) {
            int n = 0;
            for (Certificate cert : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                trust.setCertificateEntry("ca-" + n++, cert);
            }
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(null, tmf.getTrustManagers(), null);
        return ssl;
    }

    void start() {
        Thread.ofVirtual().name("pod-watch").start(this::watchLoop);
    }

    @Override
    public String mode() {
        return "kubernetes";
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public PodFeed feed() {
        return feed;
    }

    @Override
    public void kill(String name) throws IOException {
        PodView pod = pods.get(name);
        if (pod == null) {
            throw new IllegalArgumentException("no pod named " + name + " matches " + selector);
        }
        if (!pod.killable()) {
            throw new IllegalStateException(name + " is " + pod.status());
        }
        HttpResponse<String> res = send(request(URI.create(api + "/api/v1/namespaces/" + namespace + "/pods/"
                + URLEncoder.encode(name, UTF_8))).DELETE().build());
        if (res.statusCode() / 100 != 2) {
            throw new IOException("deleting " + name + " returned " + res.statusCode() + ": " + res.body());
        }
    }

    private void watchLoop() {
        while (true) {
            try {
                watch(list());
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                feed.fail("Kubernetes API: " + e.getMessage());
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /** Replaces the pod set with a fresh list and returns its resourceVersion. */
    private String list() throws IOException, InterruptedException {
        HttpResponse<String> res = http.send(request(podsUri("")).GET().build(), HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            throw new IOException("listing pods returned " + res.statusCode() + ": " + res.body());
        }
        Map<String, Object> list = JsonReader.object(res.body());
        pods.clear();
        if (list.get("items") instanceof List<?> items) {
            for (Object item : items) {
                PodView pod = toView(item);
                pods.put(pod.name(), pod);
            }
        }
        feed.publish(pods.values());
        return (String) path(list, "metadata", "resourceVersion");
    }

    /** Applies watch events until the server ends the stream. */
    private void watch(String resourceVersion) throws IOException, InterruptedException {
        URI uri = podsUri("&watch=true&timeoutSeconds=300&resourceVersion=" + URLEncoder.encode(resourceVersion, UTF_8));
        HttpResponse<Stream<String>> res = http.send(request(uri).GET().build(), HttpResponse.BodyHandlers.ofLines());
        try (Stream<String> lines = res.body()) {
            if (res.statusCode() != 200) {
                throw new IOException("watching pods returned " + res.statusCode());
            }
            for (Iterator<String> it = lines.iterator(); it.hasNext(); ) {
                String line = it.next();
                if (line.isBlank()) {
                    continue;
                }
                Map<String, Object> event = JsonReader.object(line);
                switch (String.valueOf(event.get("type"))) {
                    case "ADDED", "MODIFIED" -> {
                        PodView pod = toView(event.get("object"));
                        pods.put(pod.name(), pod);
                    }
                    case "DELETED" -> pods.remove(toView(event.get("object")).name());
                    case "ERROR" -> {
                        return; // usually 410 Gone: our resourceVersion is too old, so list again
                    }
                    default -> {
                        continue; // BOOKMARK
                    }
                }
                feed.publish(pods.values());
            }
        }
    }

    private URI podsUri(String extraQuery) {
        return URI.create(api + "/api/v1/namespaces/" + namespace + "/pods?labelSelector="
                + URLEncoder.encode(selector, UTF_8) + extraQuery);
    }

    private HttpRequest.Builder request(URI uri) {
        return HttpRequest.newBuilder(uri)
                .header("Authorization", "Bearer " + token.get())
                .header("Accept", "application/json");
    }

    private HttpResponse<String> send(HttpRequest request) throws IOException {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }

    /** Derives the same status kubectl get pods shows. */
    static PodView toView(Object pod) {
        String name = (String) path(pod, "metadata", "name");
        String created = (String) path(pod, "metadata", "creationTimestamp");
        int restarts = 0;
        boolean ready = false;
        String reason = null;
        if (path(pod, "status", "containerStatuses") instanceof List<?> containers && !containers.isEmpty()) {
            ready = true;
            for (Object c : containers) {
                if (path(c, "restartCount") instanceof Number n) {
                    restarts += n.intValue();
                }
                ready &= Boolean.TRUE.equals(path(c, "ready"));
                Object waiting = path(c, "state", "waiting", "reason");
                Object terminated = path(c, "state", "terminated", "reason");
                if (reason == null) {
                    reason = (String) (waiting != null ? waiting : terminated);
                }
            }
        }
        String status;
        if (path(pod, "metadata", "deletionTimestamp") != null) {
            status = "Terminating";
            ready = false;
        } else if (reason != null) {
            status = reason;
        } else if (path(pod, "status", "phase") instanceof String phase) {
            status = phase;
        } else {
            status = "Unknown";
        }
        return new PodView(name, status, ready, restarts,
                created == null ? 0 : Instant.parse(created).toEpochMilli(),
                (String) path(pod, "spec", "nodeName"));
    }

    private static Object path(Object root, String... keys) {
        Object cur = root;
        for (String key : keys) {
            if (!(cur instanceof Map<?, ?> map)) {
                return null;
            }
            cur = map.get(key);
        }
        return cur;
    }
}
