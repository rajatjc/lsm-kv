package io.github.rajatjc.lsmkv.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

final class TempDirs {
    static void delete(Path dir) throws IOException {
        List<Path> paths;
        try (Stream<Path> walk = Files.walk(dir)) {
            paths = walk.sorted(Comparator.reverseOrder()).toList(); // children before parents
        }
        for (Path p : paths) {
            Files.delete(p);
        }
    }

    private TempDirs() {}
}
