package io.github.rajatjc.lsmkv.cluster;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The latest pod list, versioned so each browser stream can wait for the next
 * change. Uses a lock and condition rather than synchronized/wait so waiting
 * virtual threads don't pin their carrier threads.
 */
public final class PodFeed {
    /** error is null when the last update succeeded. */
    public record Snapshot(long version, List<PodView> pods, String error) {}

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private Snapshot current = new Snapshot(0, List.of(), null);

    void publish(Collection<PodView> pods) {
        update(pods.stream().sorted(Comparator.comparing(PodView::name)).toList(), null);
    }

    /** Keeps the last known pods on screen and reports why they may be stale. */
    void fail(String error) {
        update(current().pods(), error);
    }

    private void update(List<PodView> pods, String error) {
        lock.lock();
        try {
            current = new Snapshot(current.version() + 1, pods, error);
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public Snapshot current() {
        lock.lock();
        try {
            return current;
        } finally {
            lock.unlock();
        }
    }

    /** Waits until there is a snapshot newer than version, or the timeout passes, and returns the latest. */
    public Snapshot awaitNewer(long version, Duration timeout) throws InterruptedException {
        lock.lock();
        try {
            long nanos = timeout.toNanos();
            while (current.version() <= version && nanos > 0) {
                nanos = changed.awaitNanos(nanos);
            }
            return current;
        } finally {
            lock.unlock();
        }
    }
}
