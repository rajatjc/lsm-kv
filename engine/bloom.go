package engine

import "hash/fnv"

// bloom is a fixed-size Bloom filter using double hashing (Kirsch-Mitzenmacher).
// ~10 bits per key with 7 hashes gives roughly a 1% false-positive rate.
type bloom struct {
	bits []byte
	k    uint32
}

const (
	bloomBitsPerKey = 10
	bloomHashes     = 7
)

func newBloom(n int) *bloom {
	m := n * bloomBitsPerKey
	if m < 64 {
		m = 64
	}
	return &bloom{bits: make([]byte, (m+7)/8), k: bloomHashes}
}

func bloomFromBytes(b []byte) *bloom { return &bloom{bits: b, k: bloomHashes} }

func hashes(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(key)
	s := h.Sum64()
	return uint32(s), uint32(s>>32) | 1
}

func (b *bloom) add(key []byte) {
	h1, h2 := hashes(key)
	m := uint32(len(b.bits) * 8)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + i*h2) % m
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

func (b *bloom) mayContain(key []byte) bool {
	h1, h2 := hashes(key)
	m := uint32(len(b.bits) * 8)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + i*h2) % m
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}
