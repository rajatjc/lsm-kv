package io.github.rajatjc.lsmkv.storage;

/**
 * One versioned key. A deleted entry is a tombstone: it shadows older values in
 * lower tables until compaction drops it.
 */
public record Entry(byte[] key, byte[] value, boolean deleted) {
    private static final byte[] EMPTY = new byte[0];

    static Entry put(byte[] key, byte[] value) {
        return new Entry(key, value, false);
    }

    static Entry tombstone(byte[] key) {
        return new Entry(key, EMPTY, true);
    }

    /** Bytes counted against the memtable limit. */
    int size() {
        return key.length + value.length;
    }
}
