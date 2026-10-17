package engine

import (
	"errors"
	"fmt"
	"testing"
)

func open(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(Options{Dir: dir, MemtableBytes: 512, CompactAt: 4})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestPutGetDelete(t *testing.T) {
	db := open(t, t.TempDir())
	defer db.Close()
	db.Put([]byte("a"), []byte("1"))
	if v, err := db.Get([]byte("a")); err != nil || string(v) != "1" {
		t.Fatalf("got %q, %v", v, err)
	}
	db.Delete([]byte("a"))
	if _, err := db.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestFlushCompactionAndTombstones(t *testing.T) {
	db := open(t, t.TempDir())
	defer db.Close()
	for i := 0; i < 500; i++ {
		db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	for i := 0; i < 500; i += 2 {
		db.Delete([]byte(fmt.Sprintf("key%04d", i)))
	}
	db.Put([]byte("key0001"), []byte("updated"))
	if s := db.Stats(); s.Flushes == 0 || s.Compactions == 0 {
		t.Fatalf("expected flush+compaction, got %+v", s)
	}
	db.Flush()
	db.Compact()
	if n := len(db.Stats().Tables); n != 1 {
		t.Fatalf("expected 1 table after compaction, got %d", n)
	}
	for i := 0; i < 500; i++ {
		v, err := db.Get([]byte(fmt.Sprintf("key%04d", i)))
		switch {
		case i == 1:
			if string(v) != "updated" {
				t.Fatalf("key0001 = %q", v)
			}
		case i%2 == 0:
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("key%04d should be deleted", i)
			}
		default:
			if err != nil || string(v) != fmt.Sprintf("v%d", i) {
				t.Fatalf("key%04d = %q, %v", i, v, err)
			}
		}
	}
	if got := len(db.Scan([]byte("key01"), 0)); got != 50 {
		t.Fatalf("scan key01* = %d, want 50", got)
	}
}

func TestRecoveryFromWAL(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	db.Put([]byte("x"), []byte("persisted"))
	db.Close() // no flush: only the WAL holds the data

	db = open(t, dir)
	defer db.Close()
	if v, err := db.Get([]byte("x")); err != nil || string(v) != "persisted" {
		t.Fatalf("got %q, %v", v, err)
	}
}

func TestReopenReadsSSTables(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v"))
	}
	db.Flush()
	db.Close()

	db = open(t, dir)
	defer db.Close()
	if _, err := db.Get([]byte("k050")); err != nil {
		t.Fatal(err)
	}
}

func benchPut(b *testing.B, sync bool) {
	db, _ := Open(Options{Dir: b.TempDir(), MemtableBytes: 4 << 20, SyncWrites: sync})
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put([]byte(fmt.Sprintf("key%09d", i)), []byte("value-value-value"))
	}
}

func BenchmarkPut(b *testing.B)           { benchPut(b, false) }
func BenchmarkPutSyncWrites(b *testing.B) { benchPut(b, true) }

func BenchmarkGetHit(b *testing.B) {
	db, _ := Open(Options{Dir: b.TempDir(), MemtableBytes: 1 << 20})
	defer db.Close()
	for i := 0; i < 50000; i++ {
		db.Put([]byte(fmt.Sprintf("key%09d", i)), []byte("value-value-value"))
	}
	db.Flush()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("key%09d", i%50000)))
	}
}
