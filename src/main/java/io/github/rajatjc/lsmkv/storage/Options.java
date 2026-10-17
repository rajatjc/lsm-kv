package io.github.rajatjc.lsmkv.storage;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Store configuration.
 *
 * @param dir           where the WAL and SSTables live
 * @param memtableBytes flush threshold (default 1 MB)
 * @param compactAt     compact when this many SSTables exist (default 4)
 * @param syncWrites    fsync the WAL on every write. Without it, acknowledged writes
 *                      survive a process crash (they are in the OS page cache) but
 *                      not a power loss.
 */
public record Options(Path dir, long memtableBytes, int compactAt, boolean syncWrites) {
    public Options {
        Objects.requireNonNull(dir, "dir");
        if (memtableBytes <= 0) {
            memtableBytes = 1 << 20;
        }
        if (compactAt <= 0) {
            compactAt = 4;
        }
    }

    public static Options defaults(Path dir) {
        return new Options(dir, 0, 0, false);
    }
}
