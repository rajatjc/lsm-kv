package io.github.rajatjc.lsmkv.bench;

import io.github.rajatjc.lsmkv.storage.LsmStore;
import io.github.rajatjc.lsmkv.storage.Options;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Point reads against SSTables only: hits, and misses that the Bloom filters answer. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
@State(Scope.Thread)
public class ReadBenchmark {
    private static final int KEYS = 50_000;
    private static final byte[] VALUE = "value-value-value".getBytes(UTF_8);

    private Path dir;
    private LsmStore store;
    private byte[][] hits;
    private byte[][] misses;
    private int i;

    @Setup(Level.Trial)
    public void load() throws IOException {
        dir = Files.createTempDirectory("lsm-bench");
        store = LsmStore.open(new Options(dir, 1 << 20, 4, false));
        hits = new byte[KEYS][];
        misses = new byte[KEYS][];
        for (int k = 0; k < KEYS; k++) {
            hits[k] = String.format("key%09d", k).getBytes(UTF_8);
            misses[k] = String.format("nokey%09d", k).getBytes(UTF_8);
            store.put(hits[k], VALUE);
        }
        store.flush(); // nothing left in the memtable
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
        TempDirs.delete(dir);
    }

    @Benchmark
    public Optional<byte[]> getHit() {
        i = (i + 1) % KEYS;
        return store.get(hits[i]);
    }

    @Benchmark
    public Optional<byte[]> getMiss() {
        i = (i + 1) % KEYS;
        return store.get(misses[i]);
    }
}
