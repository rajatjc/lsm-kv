package io.github.rajatjc.lsmkv.storage;

import java.nio.ByteBuffer;

/**
 * Entry encoding shared by the WAL and SSTables: op(1) | klen(4) | vlen(4) | key | value.
 * All integers on disk are little-endian.
 */
final class Format {
    static final byte OP_PUT = 1;
    static final byte OP_DELETE = 2;
    static final int ENTRY_HEADER = 9;

    static int entrySize(Entry e) {
        return ENTRY_HEADER + e.key().length + e.value().length;
    }

    static void putEntry(ByteBuffer buf, Entry e) {
        buf.put(e.deleted() ? OP_DELETE : OP_PUT)
                .putInt(e.key().length)
                .putInt(e.value().length)
                .put(e.key())
                .put(e.value());
    }

    private Format() {}
}
