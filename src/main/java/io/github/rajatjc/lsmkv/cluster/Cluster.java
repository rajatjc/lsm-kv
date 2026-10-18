package io.github.rajatjc.lsmkv.cluster;

import java.io.IOException;

/** A set of lsm-kv pods the UI can watch and kill: simulated, or real in Kubernetes. */
public interface Cluster {
    /** "demo" or "kubernetes". */
    String mode();

    String namespace();

    PodFeed feed();

    /**
     * Deletes a pod so its controller replaces it.
     *
     * @throws IllegalArgumentException if there is no such pod
     * @throws IllegalStateException    if the pod isn't running
     */
    void kill(String pod) throws IOException;
}
