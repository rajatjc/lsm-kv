package io.github.rajatjc.lsmkv.cluster;

/**
 * What the UI shows for one pod.
 *
 * @param status    kubectl-style status: Running, Pending, ContainerCreating, Terminating, CrashLoopBackOff, ...
 * @param createdAt creation time in epoch milliseconds
 * @param node      node the pod is scheduled on, or null before scheduling
 */
public record PodView(String name, String status, boolean ready, int restarts, long createdAt, String node) {
    /** Only running pods can be killed, so a pod can't be killed twice mid-restart. */
    public boolean killable() {
        return "Running".equals(status);
    }
}
