package engine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// SSTable file layout:
//
//	[data entries][sparse index][bloom filter][footer]
//
// entry:  op(1) | klen(4) | vlen(4) | key | value
// index:  repeated  klen(4) | key | offset(8)   (one per indexEvery entries)
// footer: indexOff(8) | bloomOff(8) | count(8) | magic(4)
const (
	indexEvery = 16
	footerSize = 28
	magic      = 0x4C534D31 // "LSM1"
)

type indexItem struct {
	key    []byte
	offset int64
}

type sstable struct {
	id       uint64
	path     string
	f        *os.File
	index    []indexItem
	bloom    *bloom
	indexOff int64
	count    int64
	fileSize int64
}

func encodeEntry(e Entry) []byte {
	op := opPut
	if e.Deleted {
		op = opDel
	}
	buf := make([]byte, 9+len(e.Key)+len(e.Value))
	buf[0] = op
	binary.LittleEndian.PutUint32(buf[1:], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(e.Value)))
	copy(buf[9:], e.Key)
	copy(buf[9+len(e.Key):], e.Value)
	return buf
}

func readEntry(r *bufio.Reader) (Entry, error) {
	var hdr [9]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Entry{}, err
	}
	klen := binary.LittleEndian.Uint32(hdr[1:])
	vlen := binary.LittleEndian.Uint32(hdr[5:])
	body := make([]byte, klen+vlen)
	if _, err := io.ReadFull(r, body); err != nil {
		return Entry{}, err
	}
	return Entry{Key: body[:klen], Value: body[klen:], Deleted: hdr[0] == opDel}, nil
}

// writeSSTable writes sorted entries to path and returns the opened table.
func writeSSTable(path string, id uint64, entries []Entry) (*sstable, error) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(f)
	bl := newBloom(len(entries))
	var index []indexItem
	var off int64
	for i, e := range entries {
		if i%indexEvery == 0 {
			index = append(index, indexItem{key: e.Key, offset: off})
		}
		bl.add(e.Key)
		b := encodeEntry(e)
		if _, err := w.Write(b); err != nil {
			f.Close()
			return nil, err
		}
		off += int64(len(b))
	}
	indexOff := off
	for _, it := range index {
		var h [4]byte
		binary.LittleEndian.PutUint32(h[:], uint32(len(it.key)))
		var o [8]byte
		binary.LittleEndian.PutUint64(o[:], uint64(it.offset))
		w.Write(h[:])
		w.Write(it.key)
		w.Write(o[:])
		off += int64(4 + len(it.key) + 8)
	}
	bloomOff := off
	w.Write(bl.bits)
	var foot [footerSize]byte
	binary.LittleEndian.PutUint64(foot[0:], uint64(indexOff))
	binary.LittleEndian.PutUint64(foot[8:], uint64(bloomOff))
	binary.LittleEndian.PutUint64(foot[16:], uint64(len(entries)))
	binary.LittleEndian.PutUint32(foot[24:], magic)
	w.Write(foot[:])
	if err := w.Flush(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	// fsync the directory so the rename is durable before the caller deletes
	// the WAL or the tables this one replaces.
	if err := syncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return openSSTable(path, id)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func openSSTable(path string, id uint64) (*sstable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable %s: too small", path)
	}
	var foot [footerSize]byte
	if _, err := f.ReadAt(foot[:], st.Size()-footerSize); err != nil {
		f.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint32(foot[24:]) != magic {
		f.Close()
		return nil, errors.New("sstable: bad magic")
	}
	indexOff := int64(binary.LittleEndian.Uint64(foot[0:]))
	bloomOff := int64(binary.LittleEndian.Uint64(foot[8:]))
	count := int64(binary.LittleEndian.Uint64(foot[16:]))

	idxBuf := make([]byte, bloomOff-indexOff)
	if _, err := f.ReadAt(idxBuf, indexOff); err != nil {
		f.Close()
		return nil, err
	}
	var index []indexItem
	for p := 0; p < len(idxBuf); {
		klen := int(binary.LittleEndian.Uint32(idxBuf[p:]))
		key := append([]byte(nil), idxBuf[p+4:p+4+klen]...)
		off := int64(binary.LittleEndian.Uint64(idxBuf[p+4+klen:]))
		index = append(index, indexItem{key: key, offset: off})
		p += 4 + klen + 8
	}
	bloomBuf := make([]byte, st.Size()-footerSize-bloomOff)
	if _, err := f.ReadAt(bloomBuf, bloomOff); err != nil {
		f.Close()
		return nil, err
	}
	return &sstable{
		id: id, path: path, f: f, index: index, bloom: bloomFromBytes(bloomBuf),
		indexOff: indexOff, count: count, fileSize: st.Size(),
	}, nil
}

// get returns (entry, found, bloomSkipped). A bloom miss avoids touching disk.
func (t *sstable) get(key []byte) (Entry, bool, bool) {
	if !t.bloom.mayContain(key) {
		return Entry{}, false, true
	}
	// Last index item with item.key <= key.
	i := sort.Search(len(t.index), func(i int) bool { return bytes.Compare(t.index[i].key, key) > 0 }) - 1
	if i < 0 {
		return Entry{}, false, false
	}
	start := t.index[i].offset
	end := t.indexOff
	if i+1 < len(t.index) {
		end = t.index[i+1].offset
	}
	r := bufio.NewReader(io.NewSectionReader(t.f, start, end-start))
	for {
		e, err := readEntry(r)
		if err != nil {
			return Entry{}, false, false
		}
		switch c := bytes.Compare(e.Key, key); {
		case c == 0:
			return e, true, false
		case c > 0:
			return Entry{}, false, false
		}
	}
}

type sstIter struct {
	r   *bufio.Reader
	cur Entry
	ok  bool
}

func (t *sstable) iter() *sstIter {
	it := &sstIter{r: bufio.NewReader(io.NewSectionReader(t.f, 0, t.indexOff))}
	it.next()
	return it
}

func (it *sstIter) next() {
	e, err := readEntry(it.r)
	it.cur, it.ok = e, err == nil
}

func (t *sstable) close() error { return t.f.Close() }
