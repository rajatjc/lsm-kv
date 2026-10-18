package io.github.rajatjc.lsmkv.cluster;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Demo mode: a three-replica StatefulSet that behaves like the real one when a
 * pod is killed. The pod terminates, disappears, and a replacement with the same
 * name goes through Pending, ContainerCreating and a readiness check.
 */
public final class SimulatedCluster implements Cluster {
    private static final String[] NODES = {"kind-worker", "kind-worker2"};

    private final Duration step;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "pod-simulator");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, PodView> pods = new TreeMap<>(); // guarded by this
    private final PodFeed feed = new PodFeed();
    private int nextNode;

    /** step is the length of one phase of a restart; about a second looks realistic. */
    public SimulatedCluster(int replicas, Duration step) {
        this.step = step;
        long now = System.currentTimeMillis();
        for (int i = 0; i < replicas; i++) {
            String name = "lsm-kv-" + i;
            long age = Duration.ofDays(2).minusMinutes(i).toMillis(); // created in order, two days ago
            pods.put(name, new PodView(name, "Running", true, 0, now - age, nextNode()));
        }
        feed.publish(pods.values());
    }

    @Override
    public String mode() {
        return "demo";
    }

    @Override
    public String namespace() {
        return "lsm-kv";
    }

    @Override
    public PodFeed feed() {
        return feed;
    }

    @Override
    public synchronized void kill(String name) {
        PodView pod = pods.get(name);
        if (pod == null) {
            throw new IllegalArgumentException("no pod named " + name);
        }
        if (!pod.killable()) {
            throw new IllegalStateException(name + " is " + pod.status());
        }
        put(new PodView(name, "Terminating", false, pod.restarts(), pod.createdAt(), pod.node()));
        after(2, () -> remove(name));
        // The StatefulSet controller recreates the pod under the same name.
        after(3, () -> put(new PodView(name, "Pending", false, 0, System.currentTimeMillis(), null)));
        after(4, () -> transition(name, "ContainerCreating", false, nextNode()));
        after(6, () -> transition(name, "Running", false, null)); // started, readiness probe not passed yet
        after(7, () -> transition(name, "Running", true, null));
    }

    private void after(int steps, Runnable action) {
        timer.schedule(action, step.toMillis() * steps, TimeUnit.MILLISECONDS);
    }

    private synchronized void put(PodView pod) {
        pods.put(pod.name(), pod);
        feed.publish(pods.values());
    }

    private synchronized void remove(String name) {
        pods.remove(name);
        feed.publish(pods.values());
    }

    /** Moves an existing pod to a new status, keeping its creation time; node stays put when null. */
    private synchronized void transition(String name, String status, boolean ready, String node) {
        PodView pod = pods.get(name);
        if (pod != null) {
            put(new PodView(name, status, ready, pod.restarts(), pod.createdAt(), node != null ? node : pod.node()));
        }
    }

    private String nextNode() {
        return NODES[nextNode++ % NODES.length];
    }
}
