package io.github.rajatjc.lsmkv.storage;

import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BloomFilterTest {
    @Test
    void noFalseNegativesAndAboutOnePercentFalsePositives() {
        BloomFilter bloom = BloomFilter.create(10_000);
        for (int i = 0; i < 10_000; i++) {
            bloom.add(("key" + i).getBytes(UTF_8));
        }
        for (int i = 0; i < 10_000; i++) {
            assertTrue(bloom.mayContain(("key" + i).getBytes(UTF_8)), "false negative for key" + i);
        }
        int falsePositives = 0;
        for (int i = 0; i < 100_000; i++) {
            if (bloom.mayContain(("missing" + i).getBytes(UTF_8))) {
                falsePositives++;
            }
        }
        double rate = falsePositives / 100_000.0;
        assertTrue(rate < 0.02, "false-positive rate " + rate);
    }
}
