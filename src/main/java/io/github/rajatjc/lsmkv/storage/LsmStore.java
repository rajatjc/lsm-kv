package io.github.rajatjc.lsmkv.storage;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A log-structured merge-tree key-value store:
 * WAL -> memtable -> immutable SSTables (sparse index + Bloom filter),
 * with size-tiered full compaction.
 *
 * <p>Writes, flushes and compactions take an exclusive lock; reads and scans
 * share one, so they run concurrently with each other.
 */
public final class LsmStore implements AutoCloseable {
    private static final String WAL_FILE = "wal.log";

    private final Options opts;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Wal wal;
    private Memtable mem = new Memtable();
    private List<SSTable> tables = new ArrayList<>(); // newest first
    private long nextId = 1;
    private long flushes;
    private long compactions;
    private final LongAdder bloomSkips = new LongAdder();
    private final LongAdder tableLookups = new LongAdder();

    private LsmStore(Options opts) throws IOException {
        this.opts = opts;
        Files.createDirectories(opts.dir());
        loadTables();
        this.wal = new Wal(opts.dir().resolve(WAL_FILE), opts.syncWrites());
        wal.replay(mem::put);
    }

    public static LsmStore open(Options opts) throws IOException {
        return new LsmStore(opts);
    }

    private void loadTables() throws IOException {
        List<Long> ids = new ArrayList<>();
        List<Path> leftovers = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(opts.dir())) {
            for (Path p : files) {
                String name = p.getFileName().toString();
                if (name.endsWith(".sst.tmp")) {
                    leftovers.add(p); // a crash interrupted this write; the WAL still has its data
                } else if (name.matches("\\d+\\.sst")) {
                    ids.add(Long.parseLong(name.substring(0, name.length() - 4)));
                }
            }
        }
        for (Path p : leftovers) {
            Files.delete(p);
        }
        ids.sort(Comparator.reverseOrder());
        for (long id : ids) {
            tables.add(SSTable.open(sstPath(id), id));
            nextId = Math.max(nextId, id + 1);
        }
    }

    private Path sstPath(long id) {
        return opts.dir().resolve(String.format("%06d.sst", id));
    }

    public void put(byte[] key, byte[] value) throws IOException {
        write(Entry.put(key.clone(), value.clone()));
    }

    public void delete(byte[] key) throws IOException {
        write(Entry.tombstone(key.clone()));
    }

    private void write(Entry e) throws IOException {
        if (e.key().length == 0) {
            throw new IllegalArgumentException("empty key");
        }
        lock.writeLock().lock();
        try {
            wal.append(e);
            mem.put(e);
            if (mem.bytes() >= opts.memtableBytes()) {
                flushLocked();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Returns the live value for key, or empty if it's missing or deleted. */
    public Optional<byte[]> get(byte[] key) {
        lock.readLock().lock();
        try {
            Entry e = lookup(key);
            return e == null || e.deleted() ? Optional.empty() : Optional.of(e.value().clone());
        } finally {
            lock.readLock().unlock();
        }
    }

    private Entry lookup(byte[] key) {
        Entry e = mem.get(key);
        if (e != null) {
            return e;
        }
        for (SSTable t : tables) {
            if (!t.mayContain(key)) {
                bloomSkips.increment();
                continue;
            }
            tableLookups.increment();
            if ((e = t.find(key)) != null) {
                return e;
            }
        }
        return null;
    }

    /**
     * Returns up to limit live entries whose key starts with prefix, in key order
     * (limit <= 0 means no limit). Seeks each SSTable with its sparse index and
     * stops at the end of the prefix range instead of reading whole tables.
     */
    public List<Entry> scan(byte[] prefix, int limit) {
        lock.readLock().lock();
        try {
            List<Iterator<Entry>> sources = new ArrayList<>(tables.size() + 1);
            sources.add(mem.iterator(prefix)); // newest first
            for (SSTable t : tables) {
                sources.add(t.iterator(prefix));
            }
            List<Entry> out = new ArrayList<>();
            for (MergeIterator it = new MergeIterator(sources); it.hasNext() && (limit <= 0 || out.size() < limit); ) {
                Entry e = it.next();
                if (!startsWith(e.key(), prefix)) {
                    break; // keys are sorted, so the prefix range is over
                }
                if (!e.deleted()) {
                    out.add(new Entry(e.key().clone(), e.value().clone(), false));
                }
            }
            return out;
        } finally {
            lock.readLock().unlock();
        }
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        return key.length >= prefix.length && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
    }

    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            flushLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void flushLocked() throws IOException {
        if (mem.isEmpty()) {
            return;
        }
        long id = nextId++;
        tables.add(0, SSTable.write(sstPath(id), id, mem.iterator(), mem.count()));
        mem = new Memtable();
        wal.reset();
        flushes++;
        if (tables.size() >= opts.compactAt()) {
            compactLocked();
        }
    }

    public void compact() throws IOException {
        lock.writeLock().lock();
        try {
            compactLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Merges every SSTable into one. The output is the only table left, so
     * tombstones have nothing to shadow and are dropped.
     */
    private void compactLocked() throws IOException {
        if (tables.size() < 2) {
            return;
        }
        List<Iterator<Entry>> sources = new ArrayList<>(tables.size());
        long expected = 0;
        for (SSTable t : tables) {
            sources.add(t.iterator(null));
            expected += t.count;
        }
        Iterator<Entry> live = MergeIterator.withoutTombstones(new MergeIterator(sources));
        List<SSTable> merged = new ArrayList<>(1);
        long id = nextId++;
        if (live.hasNext()) {
            merged.add(SSTable.write(sstPath(id), id, live, expected));
        }
        List<SSTable> old = tables;
        tables = merged;
        // Remove oldest first: if we crash partway, every surviving old table is
        // newer than every removed one, so a tombstone is never deleted while an
        // older value it shadows is still on disk.
        for (int i = old.size() - 1; i >= 0; i--) {
            old.get(i).close();
            Files.deleteIfExists(old.get(i).path);
        }
        compactions++;
    }

    public Stats stats() {
        lock.readLock().lock();
        try {
            List<Stats.Table> ts = tables.stream().map(t -> new Stats.Table(t.id, t.count, t.fileSize)).toList();
            return new Stats(mem.count(), mem.bytes(), opts.memtableBytes(), wal.bytes(), ts, opts.compactAt(),
                    flushes, compactions, bloomSkips.sum(), tableLookups.sum());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            for (SSTable t : tables) {
                t.close();
            }
            wal.close();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
