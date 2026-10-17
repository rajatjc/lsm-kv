package io.github.rajatjc.lsmkv.storage;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.zip.CRC32;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Write-ahead log. Record: crc32(4) | op(1) | klen(4) | vlen(4) | key | value.
 *
 * <p>The CRC covers everything after itself. Replay stops at the first torn or
 * corrupt record, which is what a crash in the middle of an append leaves behind.
 */
final class Wal implements AutoCloseable {
    private static final int RECORD_HEADER = 4 + Format.ENTRY_HEADER;

    private final Path path;
    private final boolean sync;
    private FileChannel channel;
    private long bytes;

    Wal(Path path, boolean sync) throws IOException {
        this.path = path;
        this.sync = sync;
        this.channel = FileChannel.open(path, CREATE, WRITE, APPEND);
        this.bytes = channel.size();
    }

    void append(Entry e) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4 + Format.entrySize(e)).order(ByteOrder.LITTLE_ENDIAN);
        buf.position(4);
        Format.putEntry(buf, e);
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 4, buf.capacity() - 4);
        buf.putInt(0, (int) crc.getValue());
        buf.flip();
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        if (sync) {
            channel.force(false);
        }
        bytes += buf.capacity();
    }

    long bytes() {
        return bytes;
    }

    /** Feeds every intact record, in order, to fn. */
    void replay(Consumer<Entry> fn) throws IOException {
        long remaining = Files.size(path);
        try (InputStream in = new BufferedInputStream(Files.newInputStream(path), 1 << 16)) {
            byte[] header = new byte[RECORD_HEADER];
            while (in.readNBytes(header, 0, RECORD_HEADER) == RECORD_HEADER) {
                remaining -= RECORD_HEADER;
                ByteBuffer h = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
                int klen = h.getInt(5);
                int vlen = h.getInt(9);
                if (klen < 0 || vlen < 0 || (long) klen + vlen > remaining) {
                    return; // torn tail, or garbage lengths we must not allocate for
                }
                byte[] body = in.readNBytes(klen + vlen);
                remaining -= body.length;
                CRC32 crc = new CRC32();
                crc.update(header, 4, Format.ENTRY_HEADER);
                crc.update(body);
                if ((int) crc.getValue() != h.getInt(0)) {
                    return; // corrupt tail
                }
                byte[] key = Arrays.copyOfRange(body, 0, klen);
                boolean deleted = header[4] == Format.OP_DELETE;
                fn.accept(deleted ? Entry.tombstone(key) : Entry.put(key, Arrays.copyOfRange(body, klen, body.length)));
            }
        }
    }

    /** Discards the log once its contents are durable in an SSTable. */
    void reset() throws IOException {
        channel.close();
        Files.deleteIfExists(path);
        channel = FileChannel.open(path, CREATE, WRITE, APPEND);
        bytes = 0;
    }

    @Override
    public void close() throws IOException {
        try {
            channel.force(false);
        } finally {
            channel.close();
        }
    }
}
