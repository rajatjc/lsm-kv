// Package engine implements a small LSM-tree key-value store:
// WAL -> memtable -> immutable SSTables (sparse index + Bloom filter),
// with size-tiered full compaction.
package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var ErrNotFound = errors.New("key not found")
var ErrEmptyKey = errors.New("empty key")

type Options struct {
	Dir           string
	MemtableBytes int // flush threshold
	CompactAt     int // compact when this many SSTables exist
	// SyncWrites fsyncs the WAL on every write. Without it, acknowledged writes
	// survive a process crash (they are in the OS page cache) but not power loss.
	SyncWrites bool
}

type DB struct {
	mu     sync.RWMutex
	opts   Options
	mem    *memtable
	wal    *wal
	tables []*sstable // newest first
	nextID uint64

	flushes, compactions     int
	bloomSkips, tableLookups int
}

func Open(opts Options) (*DB, error) {
	if opts.MemtableBytes <= 0 {
		opts.MemtableBytes = 1 << 20
	}
	if opts.CompactAt <= 0 {
		opts.CompactAt = 4
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{opts: opts, mem: newMemtable(), nextID: 1}

	paths, _ := filepath.Glob(filepath.Join(opts.Dir, "*.sst"))
	var ids []uint64
	for _, p := range paths {
		id, err := strconv.ParseUint(strings.TrimSuffix(filepath.Base(p), ".sst"), 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	for _, id := range ids {
		t, err := openSSTable(db.sstPath(id), id)
		if err != nil {
			return nil, err
		}
		db.tables = append(db.tables, t)
		if id >= db.nextID {
			db.nextID = id + 1
		}
	}

	w, err := openWAL(filepath.Join(opts.Dir, "wal.log"), opts.SyncWrites)
	if err != nil {
		return nil, err
	}
	if err := w.replay(func(e Entry) { db.mem.put(e) }); err != nil {
		return nil, err
	}
	db.wal = w
	return db, nil
}

func (db *DB) sstPath(id uint64) string {
	return filepath.Join(db.opts.Dir, fmt.Sprintf("%06d.sst", id))
}

func (db *DB) Put(key, value []byte) error { return db.write(Entry{Key: key, Value: value}) }
func (db *DB) Delete(key []byte) error     { return db.write(Entry{Key: key, Deleted: true}) }

func (db *DB) write(e Entry) error {
	if len(e.Key) == 0 {
		return ErrEmptyKey
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.wal.append(e); err != nil {
		return err
	}
	db.mem.put(e)
	if db.mem.size >= db.opts.MemtableBytes {
		return db.flushLocked()
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock() // counters are mutated, so take the write lock
	defer db.mu.Unlock()
	if e, ok := db.mem.get(key); ok {
		if e.Deleted {
			return nil, ErrNotFound
		}
		return e.Value, nil
	}
	for _, t := range db.tables {
		e, found, skipped := t.get(key)
		if skipped {
			db.bloomSkips++
			continue
		}
		db.tableLookups++
		if found {
			if e.Deleted {
				return nil, ErrNotFound
			}
			return e.Value, nil
		}
	}
	return nil, ErrNotFound
}

// Scan returns up to limit live entries whose key has the given prefix, in key order.
func (db *DB) Scan(prefix []byte, limit int) []Entry {
	db.mu.RLock()
	defer db.mu.RUnlock()
	merged := map[string]Entry{}
	// Oldest to newest so newer versions overwrite older ones.
	for i := len(db.tables) - 1; i >= 0; i-- {
		for it := db.tables[i].iter(); it.ok; it.next() {
			if bytes.HasPrefix(it.cur.Key, prefix) {
				merged[string(it.cur.Key)] = it.cur
			}
		}
	}
	for k, e := range db.mem.data {
		if strings.HasPrefix(k, string(prefix)) {
			merged[k] = e
		}
	}
	out := make([]Entry, 0, len(merged))
	for _, e := range merged {
		if !e.Deleted {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Key, out[j].Key) < 0 })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flushLocked()
}

func (db *DB) flushLocked() error {
	if len(db.mem.data) == 0 {
		return nil
	}
	id := db.nextID
	db.nextID++
	t, err := writeSSTable(db.sstPath(id), id, db.mem.sorted())
	if err != nil {
		return err
	}
	db.tables = append([]*sstable{t}, db.tables...)
	db.mem = newMemtable()
	if err := db.wal.reset(); err != nil {
		return err
	}
	db.flushes++
	if len(db.tables) >= db.opts.CompactAt {
		return db.compactLocked()
	}
	return nil
}

func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.compactLocked()
}

// compactLocked k-way merges every SSTable into one. Because the output is the
// bottom level, tombstones can be dropped.
func (db *DB) compactLocked() error {
	if len(db.tables) < 2 {
		return nil
	}
	iters := make([]*sstIter, len(db.tables)) // index 0 = newest
	for i, t := range db.tables {
		iters[i] = t.iter()
	}
	var out []Entry
	for {
		best := -1
		for i, it := range iters {
			if !it.ok {
				continue
			}
			if best < 0 || bytes.Compare(it.cur.Key, iters[best].cur.Key) < 0 {
				best = i // strict < keeps the newest source on ties
			}
		}
		if best < 0 {
			break
		}
		winner := iters[best].cur
		for _, it := range iters { // skip shadowed older versions
			for it.ok && bytes.Equal(it.cur.Key, winner.Key) {
				it.next()
			}
		}
		if !winner.Deleted {
			out = append(out, winner)
		}
	}

	id := db.nextID
	db.nextID++
	old := db.tables
	if len(out) == 0 {
		db.tables = nil
	} else {
		t, err := writeSSTable(db.sstPath(id), id, out)
		if err != nil {
			return err
		}
		db.tables = []*sstable{t}
	}
	// Remove oldest first: if we crash partway, every surviving old table is
	// newer than every removed one, so a tombstone is never deleted while an
	// older value it shadows is still on disk.
	for i := len(old) - 1; i >= 0; i-- {
		old[i].close()
		os.Remove(old[i].path)
	}
	db.compactions++
	return nil
}

type TableStat struct {
	ID      uint64 `json:"id"`
	Entries int64  `json:"entries"`
	Bytes   int64  `json:"bytes"`
}

type Stats struct {
	MemtableEntries int         `json:"memtableEntries"`
	MemtableBytes   int         `json:"memtableBytes"`
	MemtableLimit   int         `json:"memtableLimit"`
	WALBytes        int64       `json:"walBytes"`
	Tables          []TableStat `json:"tables"`
	CompactAt       int         `json:"compactAt"`
	Flushes         int         `json:"flushes"`
	Compactions     int         `json:"compactions"`
	BloomSkips      int         `json:"bloomSkips"`
	TableLookups    int         `json:"tableLookups"`
}

func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	s := Stats{
		MemtableEntries: len(db.mem.data), MemtableBytes: db.mem.size,
		MemtableLimit: db.opts.MemtableBytes, WALBytes: db.wal.size(),
		CompactAt: db.opts.CompactAt, Flushes: db.flushes, Compactions: db.compactions,
		BloomSkips: db.bloomSkips, TableLookups: db.tableLookups,
		Tables: []TableStat{},
	}
	for _, t := range db.tables {
		s.Tables = append(s.Tables, TableStat{ID: t.id, Entries: t.count, Bytes: t.fileSize})
	}
	return s
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, t := range db.tables {
		t.close()
	}
	return db.wal.close()
}
