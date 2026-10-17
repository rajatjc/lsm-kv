package io.github.rajatjc.lsmkv.storage;

/**
 * Bloom filter using double hashing (Kirsch-Mitzenmacher) over 64-bit FNV-1a.
 * About 10 bits per key with 7 hashes gives roughly a 1% false-positive rate.
 *
 * <p>Positions are computed with unsigned 32-bit arithmetic, so filters are
 * byte-compatible with the earlier Go implementation.
 */
final class BloomFilter {
    private static final int BITS_PER_KEY = 10;
    private static final int HASHES = 7;
    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;
    private static final long UINT32 = 0xFFFFFFFFL;

    private final byte[] bits;

    private BloomFilter(byte[] bits) {
        this.bits = bits;
    }

    static BloomFilter create(long expectedKeys) {
        long m = Math.max(64, expectedKeys * BITS_PER_KEY);
        return new BloomFilter(new byte[(int) ((m + 7) / 8)]);
    }

    static BloomFilter fromBytes(byte[] bits) {
        return new BloomFilter(bits);
    }

    byte[] bytes() {
        return bits;
    }

    void add(byte[] key) {
        long h = fnv1a(key);
        long h1 = h & UINT32;
        long h2 = (h >>> 32) | 1;
        long m = bits.length * 8L;
        for (int i = 0; i < HASHES; i++) {
            long pos = ((h1 + i * h2) & UINT32) % m;
            bits[(int) (pos >>> 3)] |= (byte) (1 << (pos & 7));
        }
    }

    boolean mayContain(byte[] key) {
        long h = fnv1a(key);
        long h1 = h & UINT32;
        long h2 = (h >>> 32) | 1;
        long m = bits.length * 8L;
        for (int i = 0; i < HASHES; i++) {
            long pos = ((h1 + i * h2) & UINT32) % m;
            if ((bits[(int) (pos >>> 3)] & (1 << (pos & 7))) == 0) {
                return false;
            }
        }
        return true;
    }

    private static long fnv1a(byte[] key) {
        long h = FNV_OFFSET;
        for (byte b : key) {
            h ^= b & 0xff;
            h *= FNV_PRIME;
        }
        return h;
    }
}
