package engine

import "sort"

// Entry is a single versioned key. Deleted entries are tombstones that shadow
// older values in lower tables until compaction drops them.
type Entry struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

type memtable struct {
	data map[string]Entry
	size int
}

func newMemtable() *memtable {
	return &memtable{data: make(map[string]Entry)}
}

func (m *memtable) put(e Entry) {
	if old, ok := m.data[string(e.Key)]; ok {
		m.size -= len(old.Key) + len(old.Value)
	}
	m.data[string(e.Key)] = e
	m.size += len(e.Key) + len(e.Value)
}

func (m *memtable) get(key []byte) (Entry, bool) {
	e, ok := m.data[string(key)]
	return e, ok
}

// sorted returns entries in key order, ready to be written as an SSTable.
func (m *memtable) sorted() []Entry {
	out := make([]Entry, 0, len(m.data))
	for _, e := range m.data {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].Key) < string(out[j].Key) })
	return out
}
