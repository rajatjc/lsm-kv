package io.github.rajatjc.lsmkv.server;

import java.util.List;
import java.util.StringJoiner;

/** Minimal JSON output. The API only emits objects, arrays, strings and numbers. */
final class Json {
    /** Already-rendered JSON, embedded as is. */
    record Raw(String json) {}

    /** Builds an object from alternating keys and values (String, Number, Boolean or Raw). */
    static String object(Object... pairs) {
        StringJoiner out = new StringJoiner(",", "{", "}");
        for (int i = 0; i < pairs.length; i += 2) {
            out.add(string((String) pairs[i]) + ":" + value(pairs[i + 1]));
        }
        return out.toString();
    }

    static String array(List<String> items) {
        return "[" + String.join(",", items) + "]";
    }

    static String string(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    private static String value(Object v) {
        return switch (v) {
            case String s -> string(s);
            case Raw r -> r.json();
            case Number n -> n.toString();
            case Boolean b -> b.toString();
            default -> throw new IllegalArgumentException("unsupported JSON value: " + v);
        };
    }

    private Json() {}
}
