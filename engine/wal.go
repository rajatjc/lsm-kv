package engine

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// WAL record: crc32(4) | op(1) | klen(4) | vlen(4) | key | value
// The CRC covers everything after itself. A torn or corrupt tail record is
// treated as the end of the log, which is the normal result of a crash
// mid-append.
type wal struct {
	f    *os.File
	path string
	sync bool // fsync after every append (durable across power loss, much slower)
}

const (
	opPut byte = 1
	opDel byte = 2
)

func openWAL(path string, sync bool) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{f: f, path: path, sync: sync}, nil
}

func (w *wal) append(e Entry) error {
	op := opPut
	if e.Deleted {
		op = opDel
	}
	buf := make([]byte, 13+len(e.Key)+len(e.Value))
	buf[4] = op
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(buf[9:], uint32(len(e.Value)))
	copy(buf[13:], e.Key)
	copy(buf[13+len(e.Key):], e.Value)
	binary.LittleEndian.PutUint32(buf[0:], crc32.ChecksumIEEE(buf[4:]))
	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func (w *wal) size() int64 {
	st, err := w.f.Stat()
	if err != nil {
		return 0
	}
	return st.Size()
}

// replay feeds every intact record to fn.
func (w *wal) replay(fn func(Entry)) error {
	f, err := os.Open(w.path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		hdr := make([]byte, 13)
		if _, err := io.ReadFull(r, hdr); err != nil {
			return nil // EOF or torn header
		}
		klen := binary.LittleEndian.Uint32(hdr[5:])
		vlen := binary.LittleEndian.Uint32(hdr[9:])
		body := make([]byte, klen+vlen)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil // torn body
		}
		sum := crc32.NewIEEE()
		sum.Write(hdr[4:])
		sum.Write(body)
		if sum.Sum32() != binary.LittleEndian.Uint32(hdr[0:]) {
			return nil // corrupt tail
		}
		fn(Entry{Key: body[:klen], Value: body[klen:], Deleted: hdr[4] == opDel})
	}
}

// reset discards the log after its contents are durable in an SSTable.
func (w *wal) reset() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	if err := os.Remove(w.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.f = f
	return nil
}

func (w *wal) close() error {
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}
