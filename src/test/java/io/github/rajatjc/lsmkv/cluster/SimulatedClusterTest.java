package io.github.rajatjc.lsmkv.cluster;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimulatedClusterTest {
    @Test
    void killedPodIsReplacedUnderTheSameName() throws Exception {
        SimulatedCluster cluster = new SimulatedCluster(3, Duration.ofMillis(20));
        PodView before = pod(cluster, "lsm-kv-1");
        assertEquals("Running", before.status());

        cluster.kill("lsm-kv-1");
        assertEquals("Terminating", pod(cluster, "lsm-kv-1").status());
        assertThrows(IllegalStateException.class, () -> cluster.kill("lsm-kv-1"), "can't kill a pod twice");

        // Record every state the pod passes through until it's ready again.
        List<String> seen = new ArrayList<>();
        PodFeed.Snapshot s = cluster.feed().current();
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            s = cluster.feed().awaitNewer(s.version(), Duration.ofSeconds(1));
            PodView p = s.pods().stream().filter(x -> x.name().equals("lsm-kv-1")).findFirst().orElse(null);
            seen.add(p == null ? "gone" : p.status() + (p.ready() ? "+ready" : ""));
            if (p != null && p.ready()) {
                break;
            }
        }
        assertEquals(List.of("gone", "Pending", "ContainerCreating", "Running", "Running+ready"), seen);
        assertTrue(pod(cluster, "lsm-kv-1").createdAt() > before.createdAt(), "replacement is a new pod");
        assertEquals(3, cluster.feed().current().pods().size());
    }

    private static PodView pod(SimulatedCluster cluster, String name) {
        return cluster.feed().current().pods().stream().filter(p -> p.name().equals(name)).findFirst().orElseThrow();
    }
}
