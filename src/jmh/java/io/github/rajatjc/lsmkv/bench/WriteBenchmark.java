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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Write path: WAL append + memtable insert, with flushes amortized in. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
@State(Scope.Thread)
public class WriteBenchmark {
    private static final byte[] VALUE = "value-value-value".getBytes(UTF_8);

    @Param({"false", "true"})
    public boolean syncWrites;

    private Path dir;
    private LsmStore store;
    private long i;

    @Setup(Level.Trial)
    public void open() throws IOException {
        dir = Files.createTempDirectory("lsm-bench");
        // Compaction is off so this measures writes, not the merge cost of an ever-growing table.
        store = LsmStore.open(new Options(dir, 4 << 20, Integer.MAX_VALUE, syncWrites));
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
        TempDirs.delete(dir);
    }

    @Benchmark
    public void put() throws IOException {
        store.put(("key" + i++).getBytes(UTF_8), VALUE);
    }
}
