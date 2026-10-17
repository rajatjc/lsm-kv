package io.github.rajatjc.lsmkv.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LsmStoreTest {
    @TempDir
    Path dir;

    /** Tiny memtable, so a few hundred writes cause flushes and compactions. */
    private LsmStore open() throws IOException {
        return LsmStore.open(new Options(dir, 512, 4, false));
    }

    private static byte[] b(String s) {
        return s.getBytes(UTF_8);
    }

    private static String get(LsmStore store, String key) {
        return store.get(b(key)).map(v -> new String(v, UTF_8)).orElse(null);
    }

    private static String key(int i) {
        return String.format("key%04d", i);
    }

    @Test
    void putGetDelete() throws IOException {
        try (LsmStore store = open()) {
            store.put(b("a"), b("1"));
            assertEquals("1", get(store, "a"));
            store.delete(b("a"));
            assertNull(get(store, "a"));
        }
    }

    @Test
    void flushCompactionAndTombstones() throws IOException {
        try (LsmStore store = open()) {
            for (int i = 0; i < 500; i++) {
                store.put(b(key(i)), b("v" + i));
            }
            for (int i = 0; i < 500; i += 2) {
                store.delete(b(key(i)));
            }
            store.put(b("key0001"), b("updated"));
            Stats s = store.stats();
            assertTrue(s.flushes() > 0 && s.compactions() > 0, "expected flushes and compactions: " + s);
            assertEquals(50, store.scan(b("key01"), 0).size(), "scan across memtable and several tables");

            store.flush();
            store.compact();
            assertEquals(1, store.stats().tables().size());
            for (int i = 0; i < 500; i++) {
                String expected = i == 1 ? "updated" : i % 2 == 0 ? null : "v" + i;
                assertEquals(expected, get(store, key(i)), key(i));
            }
            assertEquals(50, store.scan(b("key01"), 0).size(), "scan after full compaction");
        }
    }

    @Test
    void scanMergesSourcesNewestFirst() throws IOException {
        try (LsmStore store = LsmStore.open(new Options(dir, 1 << 20, 100, false))) {
            store.put(b("user:1"), b("old"));
            store.put(b("user:2"), b("two"));
            store.put(b("other"), b("x"));
            store.flush();
            store.put(b("user:1"), b("new"));
            store.delete(b("user:2"));
            store.put(b("user:3"), b("three"));
            store.flush();
            store.put(b("user:0"), b("zero"));       // memtable only
            store.put(b("user:3"), b("three-v2"));   // memtable shadows the newer table

            List<String> rows = store.scan(b("user:"), 0).stream()
                    .map(e -> new String(e.key(), UTF_8) + "=" + new String(e.value(), UTF_8))
                    .toList();
            assertEquals(List.of("user:0=zero", "user:1=new", "user:3=three-v2"), rows);
            assertEquals(2, store.scan(b("user:"), 2).size());
            assertTrue(store.scan(b("nope"), 0).isEmpty());
        }
    }

    @Test
    void recoversUnflushedWritesFromWal() throws IOException {
        try (LsmStore store = open()) {
            store.put(b("x"), b("persisted"));
        }
        try (LsmStore store = open()) {
            assertEquals("persisted", get(store, "x"));
        }
    }

    @Test
    void ignoresTornWalTail() throws IOException {
        try (LsmStore store = open()) {
            store.put(b("a"), b("1"));
            store.put(b("b"), b("2"));
        }
        try (FileChannel wal = FileChannel.open(dir.resolve("wal.log"), StandardOpenOption.WRITE)) {
            wal.truncate(wal.size() - 3); // a crash in the middle of the last append
        }
        try (LsmStore store = open()) {
            assertEquals("1", get(store, "a"));
            assertNull(get(store, "b"));
        }
    }

    @Test
    void reopensSSTables() throws IOException {
        try (LsmStore store = open()) {
            for (int i = 0; i < 100; i++) {
                store.put(b("k" + i), b("v" + i));
            }
            store.flush();
        }
        try (LsmStore store = open()) {
            assertFalse(store.stats().tables().isEmpty());
            assertEquals("v50", get(store, "k50"));
        }
    }

    @Test
    void deletesTempFilesLeftByInterruptedWrites() throws IOException {
        Files.writeString(dir.resolve("000099.sst.tmp"), "partial");
        try (LsmStore store = open()) {
            assertFalse(Files.exists(dir.resolve("000099.sst.tmp")));
        }
    }
}
