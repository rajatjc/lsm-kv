package io.github.rajatjc.lsmkv.storage;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory write buffer. It stays sorted, so flushing and range scans need no
 * sort step. Mutated only under the store's write lock.
 */
final class Memtable {
    private final ConcurrentSkipListMap<byte[], Entry> map = new ConcurrentSkipListMap<>(Arrays::compareUnsigned);
    private int count;
    private long bytes;

    void put(Entry e) {
        Entry old = map.put(e.key(), e);
        if (old == null) {
            count++;
        } else {
            bytes -= old.size();
        }
        bytes += e.size();
    }

    Entry get(byte[] key) {
        return map.get(key);
    }

    Iterator<Entry> iterator() {
        return map.values().iterator();
    }

    /** Entries with key >= from, in key order. */
    Iterator<Entry> iterator(byte[] from) {
        return map.tailMap(from, true).values().iterator();
    }

    int count() {
        return count;
    }

    long bytes() {
        return bytes;
    }

    boolean isEmpty() {
        return count == 0;
    }
}
