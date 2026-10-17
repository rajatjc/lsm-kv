package io.github.rajatjc.lsmkv.storage;

import java.util.List;

/** Point-in-time view of the store, for the API and the UI. Tables are newest first. */
public record Stats(int memtableEntries, long memtableBytes, long memtableLimit, long walBytes,
                    List<Table> tables, int compactAt, long flushes, long compactions,
                    long bloomSkips, long tableLookups) {

    public record Table(long id, long entries, long bytes) {}

    public long dataBytes() {
        return memtableBytes + walBytes + tables.stream().mapToLong(Table::bytes).sum();
    }
}
