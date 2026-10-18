package io.github.rajatjc.lsmkv.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Small JSON parser for Kubernetes API responses. Objects become Maps, arrays
 * Lists, integers Longs and other numbers Doubles.
 */
final class JsonReader {
    private final String s;
    private int i;

    private JsonReader(String s) {
        this.s = s;
    }

    static Object parse(String json) {
        JsonReader r = new JsonReader(json);
        r.skipWhitespace();
        Object value = r.value();
        r.skipWhitespace();
        if (r.i != r.s.length()) {
            throw r.error("trailing data");
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(String json) {
        if (parse(json) instanceof Map<?, ?> m) {
            return (Map<String, Object>) m;
        }
        throw new IllegalArgumentException("expected a JSON object");
    }

    private Object value() {
        if (i >= s.length()) {
            throw error("unexpected end of input");
        }
        return switch (s.charAt(i)) {
            case '{' -> object();
            case '[' -> array();
            case '"' -> string();
            case 't' -> literal("true", Boolean.TRUE);
            case 'f' -> literal("false", Boolean.FALSE);
            case 'n' -> literal("null", null);
            default -> number();
        };
    }

    private Map<String, Object> object() {
        Map<String, Object> out = new LinkedHashMap<>();
        i++; // {
        skipWhitespace();
        if (peek() == '}') {
            i++;
            return out;
        }
        while (true) {
            skipWhitespace();
            String key = string();
            skipWhitespace();
            expect(':');
            skipWhitespace();
            out.put(key, value());
            skipWhitespace();
            if (peek() == ',') {
                i++;
            } else {
                expect('}');
                return out;
            }
        }
    }

    private List<Object> array() {
        List<Object> out = new ArrayList<>();
        i++; // [
        skipWhitespace();
        if (peek() == ']') {
            i++;
            return out;
        }
        while (true) {
            skipWhitespace();
            out.add(value());
            skipWhitespace();
            if (peek() == ',') {
                i++;
            } else {
                expect(']');
                return out;
            }
        }
    }

    private String string() {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (i >= s.length()) {
                throw error("unterminated string");
            }
            char c = s.charAt(i++);
            if (c == '"') {
                return sb.toString();
            }
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            if (i >= s.length()) {
                throw error("unterminated escape");
            }
            char e = s.charAt(i++);
            switch (e) {
                case '"', '\\', '/' -> sb.append(e);
                case 'b' -> sb.append('\b');
                case 'f' -> sb.append('\f');
                case 'n' -> sb.append('\n');
                case 'r' -> sb.append('\r');
                case 't' -> sb.append('\t');
                case 'u' -> {
                    if (i + 4 > s.length()) {
                        throw error("bad unicode escape");
                    }
                    sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                    i += 4;
                }
                default -> throw error("bad escape \\" + e);
            }
        }
    }

    private Object number() {
        int start = i;
        while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) {
            i++;
        }
        String text = s.substring(start, i);
        if (text.isEmpty()) {
            throw error("unexpected character '" + s.charAt(i) + "'");
        }
        try {
            boolean decimal = text.contains(".") || text.contains("e") || text.contains("E");
            return decimal ? (Object) Double.parseDouble(text) : (Object) Long.parseLong(text);
        } catch (NumberFormatException e) {
            return Double.parseDouble(text); // integers too big for a long
        }
    }

    private Object literal(String word, Object value) {
        if (!s.startsWith(word, i)) {
            throw error("unexpected token");
        }
        i += word.length();
        return value;
    }

    private char peek() {
        if (i >= s.length()) {
            throw error("unexpected end of input");
        }
        return s.charAt(i);
    }

    private void expect(char c) {
        if (peek() != c) {
            throw error("expected '" + c + "'");
        }
        i++;
    }

    private void skipWhitespace() {
        while (i < s.length() && " \t\n\r".indexOf(s.charAt(i)) >= 0) {
            i++;
        }
    }

    private IllegalArgumentException error(String message) {
        return new IllegalArgumentException(message + " at offset " + i);
    }
}
