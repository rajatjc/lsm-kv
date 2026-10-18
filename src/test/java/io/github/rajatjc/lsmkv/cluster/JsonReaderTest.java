package io.github.rajatjc.lsmkv.cluster;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonReaderTest {
    @Test
    void parsesNestedValues() {
        Map<String, Object> o = JsonReader.object("""
                {"a": 1, "b": -2.5e1, "c": "x\\"y\\u0041\\n", "d": [true, false, null, {}], "e": {"f": []}}
                """);
        assertEquals(1L, o.get("a"));
        assertEquals(-25.0, o.get("b"));
        assertEquals("x\"yA\n", o.get("c"));
        assertEquals(List.of(true, false), ((List<?>) o.get("d")).subList(0, 2));
        assertNull(((List<?>) o.get("d")).get(2));
        assertEquals(Map.of("f", List.of()), o.get("e"));
    }

    @Test
    void rejectsMalformedInput() {
        assertThrows(IllegalArgumentException.class, () -> JsonReader.parse("{\"a\": }"));
        assertThrows(IllegalArgumentException.class, () -> JsonReader.parse("[1, 2"));
        assertThrows(IllegalArgumentException.class, () -> JsonReader.parse("\"open"));
        assertThrows(IllegalArgumentException.class, () -> JsonReader.parse("{} extra"));
    }
}
