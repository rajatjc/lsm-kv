package io.github.rajatjc.lsmkv.storage;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * K-way merge of sorted sources, ordered newest first. Yields each key once,
 * taking the version from the newest source that has it. Tombstones are
 * returned too; callers decide whether to keep them.
 */
final class MergeIterator implements Iterator<Entry> {
    private record Head(Entry entry, int source, Iterator<Entry> rest) {}

    private final PriorityQueue<Head> heap = new PriorityQueue<>((a, b) -> {
        int c = Arrays.compareUnsigned(a.entry().key(), b.entry().key());
        return c != 0 ? c : Integer.compare(a.source(), b.source()); // newer source first on ties
    });

    MergeIterator(List<Iterator<Entry>> sources) {
        for (int i = 0; i < sources.size(); i++) {
            push(i, sources.get(i));
        }
    }

    private void push(int source, Iterator<Entry> it) {
        if (it.hasNext()) {
            heap.add(new Head(it.next(), source, it));
        }
    }

    @Override
    public boolean hasNext() {
        return !heap.isEmpty();
    }

    @Override
    public Entry next() {
        Head top = heap.poll();
        if (top == null) {
            throw new NoSuchElementException();
        }
        push(top.source(), top.rest());
        // Older versions of the same key are shadowed; skip past them.
        while (!heap.isEmpty() && Arrays.equals(heap.peek().entry().key(), top.entry().key())) {
            Head older = heap.poll();
            push(older.source(), older.rest());
        }
        return top.entry();
    }

    /** Drops tombstones, for output that has no older tables left to shadow. */
    static Iterator<Entry> withoutTombstones(Iterator<Entry> it) {
        return new Iterator<>() {
            private Entry next = advance();

            private Entry advance() {
                while (it.hasNext()) {
                    Entry e = it.next();
                    if (!e.deleted()) {
                        return e;
                    }
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
}
