package io.github.rajatjc.lsmkv.storage;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Immutable sorted table on disk.
 *
 * <pre>
 * [data entries][sparse index][bloom filter][footer]
 *
 * entry:  op(1) | klen(4) | vlen(4) | key | value
 * index:  klen(4) | key | offset(8), one per INDEX_EVERY entries
 * footer: indexOffset(8) | bloomOffset(8) | count(8) | magic(4)
 * </pre>
 *
 * The file is memory-mapped, so lookups and scans read it without system calls.
 * All reads use absolute positions, which keeps the shared buffer safe for
 * concurrent readers.
 */
final class SSTable implements AutoCloseable {
    static final int INDEX_EVERY = 16;
    private static final int FOOTER_SIZE = 28;
    private static final int MAGIC = 0x4C534D31; // "LSM1"

    final long id;
    final Path path;
    final long count;
    final long fileSize;
    private final FileChannel channel;
    private final ByteBuffer data;
    private final int indexOffset;
    private final byte[][] indexKeys;
    private final int[] indexOffsets;
    private final BloomFilter bloom;

    private SSTable(long id, Path path, long count, long fileSize, FileChannel channel, ByteBuffer data,
                    int indexOffset, byte[][] indexKeys, int[] indexOffsets, BloomFilter bloom) {
        this.id = id;
        this.path = path;
        this.count = count;
        this.fileSize = fileSize;
        this.channel = channel;
        this.data = data;
        this.indexOffset = indexOffset;
        this.indexKeys = indexKeys;
        this.indexOffsets = indexOffsets;
        this.bloom = bloom;
    }

    /**
     * Writes entries (sorted, unique keys) to path and opens the result.
     * expectedCount sizes the Bloom filter and may overestimate.
     */
    static SSTable write(Path path, long id, Iterator<Entry> entries, long expectedCount) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        BloomFilter bloom = BloomFilter.create(expectedCount);
        List<byte[]> indexKeys = new ArrayList<>();
        List<Long> indexOffsets = new ArrayList<>();
        long offset = 0;
        long count = 0;
        try (FileChannel ch = FileChannel.open(tmp, CREATE, TRUNCATE_EXISTING, WRITE);
             OutputStream out = new BufferedOutputStream(Channels.newOutputStream(ch), 1 << 16)) {
            while (entries.hasNext()) {
                Entry e = entries.next();
                if (count++ % INDEX_EVERY == 0) {
                    indexKeys.add(e.key());
                    indexOffsets.add(offset);
                }
                bloom.add(e.key());
                ByteBuffer buf = ByteBuffer.allocate(Format.entrySize(e)).order(ByteOrder.LITTLE_ENDIAN);
                Format.putEntry(buf, e);
                out.write(buf.array());
                offset += buf.capacity();
            }
            long dataEnd = offset;
            for (int i = 0; i < indexKeys.size(); i++) {
                byte[] key = indexKeys.get(i);
                ByteBuffer buf = ByteBuffer.allocate(4 + key.length + 8).order(ByteOrder.LITTLE_ENDIAN);
                buf.putInt(key.length).put(key).putLong(indexOffsets.get(i));
                out.write(buf.array());
                offset += buf.capacity();
            }
            long bloomOffset = offset;
            out.write(bloom.bytes());
            ByteBuffer footer = ByteBuffer.allocate(FOOTER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            footer.putLong(dataEnd).putLong(bloomOffset).putLong(count).putInt(MAGIC);
            out.write(footer.array());
            out.flush();
            ch.force(true);
        }
        Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE);
        // fsync the directory so the rename is durable before the caller deletes
        // the WAL or the tables this one replaces.
        syncDirectory(path.getParent());
        return open(path, id);
    }

    static SSTable open(Path path, long id) throws IOException {
        FileChannel ch = FileChannel.open(path, READ);
        try {
            long size = ch.size();
            if (size < FOOTER_SIZE || size > Integer.MAX_VALUE) {
                throw new IOException(path + ": unsupported size " + size);
            }
            ByteBuffer data = ch.map(FileChannel.MapMode.READ_ONLY, 0, size).order(ByteOrder.LITTLE_ENDIAN);
            int footer = (int) size - FOOTER_SIZE;
            if (data.getInt(footer + 24) != MAGIC) {
                throw new IOException(path + ": bad magic");
            }
            int indexOffset = (int) data.getLong(footer);
            int bloomOffset = (int) data.getLong(footer + 8);
            long count = data.getLong(footer + 16);

            List<byte[]> keys = new ArrayList<>();
            List<Integer> offsets = new ArrayList<>();
            for (int p = indexOffset; p < bloomOffset; ) {
                int klen = data.getInt(p);
                byte[] key = new byte[klen];
                data.get(p + 4, key);
                keys.add(key);
                offsets.add((int) data.getLong(p + 4 + klen));
                p += 4 + klen + 8;
            }
            byte[] bloomBits = new byte[footer - bloomOffset];
            data.get(bloomOffset, bloomBits);
            return new SSTable(id, path, count, size, ch, data, indexOffset, keys.toArray(byte[][]::new),
                    offsets.stream().mapToInt(Integer::intValue).toArray(), BloomFilter.fromBytes(bloomBits));
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    boolean mayContain(byte[] key) {
        return bloom.mayContain(key);
    }

    /** Returns the entry for key, or null if this table doesn't have one. */
    Entry find(byte[] key) {
        int block = floorBlock(key);
        if (block < 0) {
            return null;
        }
        int end = block + 1 < indexOffsets.length ? indexOffsets[block + 1] : indexOffset;
        for (int p = indexOffsets[block]; p < end; ) {
            int klen = data.getInt(p + 1);
            int c = compareKeyAt(p + Format.ENTRY_HEADER, klen, key);
            if (c == 0) {
                return readEntry(p);
            }
            if (c > 0) {
                return null;
            }
            p += Format.ENTRY_HEADER + klen + data.getInt(p + 5);
        }
        return null;
    }

    /** Entries with key >= from (all entries when from is null), in key order. */
    Iterator<Entry> iterator(byte[] from) {
        int start = 0;
        if (from != null) {
            int block = floorBlock(from);
            if (block >= 0) {
                start = indexOffsets[block]; // jump to the block that could hold from
            }
        }
        int begin = start;
        return new Iterator<>() {
            private int p = begin;
            private boolean seeking = from != null;
            private Entry next = advance();

            private Entry advance() {
                while (p < indexOffset) {
                    int at = p;
                    int klen = data.getInt(at + 1);
                    p += Format.ENTRY_HEADER + klen + data.getInt(at + 5);
                    if (seeking && compareKeyAt(at + Format.ENTRY_HEADER, klen, from) < 0) {
                        continue;
                    }
                    seeking = false;
                    return readEntry(at);
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Entry next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                Entry e = next;
                next = advance();
                return e;
            }
        };
    }

    /** Index of the last sparse-index block whose first key is <= key, or -1. */
    private int floorBlock(byte[] key) {
        int lo = 0;
        int hi = indexKeys.length - 1;
        int found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (Arrays.compareUnsigned(indexKeys[mid], key) <= 0) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return found;
    }

    /** Compares the key stored at pos with key, without copying it out of the map. */
    private int compareKeyAt(int pos, int len, byte[] key) {
        int n = Math.min(len, key.length);
        for (int i = 0; i < n; i++) {
            int c = Byte.compareUnsigned(data.get(pos + i), key[i]);
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(len, key.length);
    }

    private Entry readEntry(int p) {
        int klen = data.getInt(p + 1);
        int vlen = data.getInt(p + 5);
        byte[] key = new byte[klen];
        byte[] value = new byte[vlen];
        data.get(p + Format.ENTRY_HEADER, key);
        data.get(p + Format.ENTRY_HEADER + klen, value);
        return new Entry(key, value, data.get(p) == Format.OP_DELETE);
    }

    static void syncDirectory(Path dir) throws IOException {
        try (FileChannel ch = FileChannel.open(dir, READ)) {
            ch.force(true);
        }
    }

    /** The mapping stays valid until garbage-collected, even after the file is deleted. */
    @Override
    public void close() throws IOException {
        channel.close();
    }
}
