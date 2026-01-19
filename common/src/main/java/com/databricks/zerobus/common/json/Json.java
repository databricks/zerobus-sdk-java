package com.databricks.zerobus.common.json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Minimal JSON utility for parsing and serializing JSON without external dependencies.
 *
 * <p>This class provides basic JSON operations sufficient for SDK needs without adding a dependency
 * on a JSON library.
 *
 * <p>Supported types:
 *
 * <ul>
 *   <li>Objects → {@code Map<String, Object>}
 *   <li>Arrays → {@code List<Object>}
 *   <li>Strings → {@code String}
 *   <li>Numbers → {@code Integer}, {@code Long}, or {@code Double}
 *   <li>Booleans → {@code Boolean}
 *   <li>Null → {@code null}
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Parsing
 * Map<String, Object> obj = (Map<String, Object>) Json.parse("{\"name\": \"Alice\"}");
 *
 * // Serializing
 * String json = Json.stringify(Map.of("name", "Alice", "age", 30));
 * }</pre>
 */
public final class Json {

  private Json() {}

  /**
   * Parses a JSON string into a Java object.
   *
   * @param json The JSON string to parse
   * @return The parsed object (Map, List, String, Number, Boolean, or null)
   * @throws IllegalArgumentException if the JSON is malformed
   */
  @Nullable public static Object parse(@Nonnull String json) {
    return new Parser(json).parse();
  }

  /**
   * Serializes a Java object to a JSON string.
   *
   * <p>Supported types:
   *
   * <ul>
   *   <li>{@code Map<String, ?>} → JSON object
   *   <li>{@code Iterable<?>} → JSON array
   *   <li>{@code String} → JSON string
   *   <li>{@code Number} → JSON number
   *   <li>{@code Boolean} → JSON boolean
   *   <li>{@code null} → JSON null
   * </ul>
   *
   * @param obj The object to serialize
   * @return The JSON string representation
   * @throws IllegalArgumentException if the object type is not supported
   */
  @Nonnull
  public static String stringify(@Nullable Object obj) {
    StringBuilder sb = new StringBuilder();
    new Writer(sb).write(obj);
    return sb.toString();
  }

  // ==================== Parser ====================

  /** Minimal JSON parser that handles objects, arrays, strings, numbers, booleans, and nulls. */
  private static class Parser {
    private final String json;
    private int pos = 0;

    Parser(String json) {
      this.json = json.trim();
    }

    Object parse() {
      skipWhitespace();
      return parseValue();
    }

    private Object parseValue() {
      skipWhitespace();
      char c = peek();

      if (c == '{') {
        return parseObject();
      } else if (c == '[') {
        return parseArray();
      } else if (c == '"') {
        return parseString();
      } else if (c == 't' || c == 'f') {
        return parseBoolean();
      } else if (c == 'n') {
        return parseNull();
      } else if (c == '-' || Character.isDigit(c)) {
        return parseNumber();
      } else {
        throw new IllegalArgumentException("Unexpected character at position " + pos + ": " + c);
      }
    }

    private Map<String, Object> parseObject() {
      Map<String, Object> map = new HashMap<>();
      consume('{');
      skipWhitespace();

      if (peek() == '}') {
        consume('}');
        return map;
      }

      while (true) {
        skipWhitespace();
        String key = parseString();
        skipWhitespace();
        consume(':');
        skipWhitespace();
        Object value = parseValue();
        map.put(key, value);

        skipWhitespace();
        char c = peek();
        if (c == '}') {
          consume('}');
          break;
        } else if (c == ',') {
          consume(',');
        } else {
          throw new IllegalArgumentException("Expected ',' or '}' at position " + pos);
        }
      }

      return map;
    }

    private List<Object> parseArray() {
      List<Object> list = new ArrayList<>();
      consume('[');
      skipWhitespace();

      if (peek() == ']') {
        consume(']');
        return list;
      }

      while (true) {
        skipWhitespace();
        list.add(parseValue());
        skipWhitespace();

        char c = peek();
        if (c == ']') {
          consume(']');
          break;
        } else if (c == ',') {
          consume(',');
        } else {
          throw new IllegalArgumentException("Expected ',' or ']' at position " + pos);
        }
      }

      return list;
    }

    private String parseString() {
      consume('"');
      StringBuilder sb = new StringBuilder();

      while (pos < json.length()) {
        char c = json.charAt(pos);
        if (c == '"') {
          pos++;
          return sb.toString();
        } else if (c == '\\') {
          pos++;
          if (pos >= json.length()) {
            throw new IllegalArgumentException("Unterminated string escape");
          }
          char escaped = json.charAt(pos);
          switch (escaped) {
            case '"':
            case '\\':
            case '/':
              sb.append(escaped);
              break;
            case 'b':
              sb.append('\b');
              break;
            case 'f':
              sb.append('\f');
              break;
            case 'n':
              sb.append('\n');
              break;
            case 'r':
              sb.append('\r');
              break;
            case 't':
              sb.append('\t');
              break;
            case 'u':
              if (pos + 4 >= json.length()) {
                throw new IllegalArgumentException("Invalid unicode escape");
              }
              String hex = json.substring(pos + 1, pos + 5);
              sb.append((char) Integer.parseInt(hex, 16));
              pos += 4;
              break;
            default:
              throw new IllegalArgumentException("Invalid escape character: " + escaped);
          }
          pos++;
        } else {
          sb.append(c);
          pos++;
        }
      }

      throw new IllegalArgumentException("Unterminated string");
    }

    private Object parseNumber() {
      int start = pos;
      if (peek() == '-') {
        pos++;
      }

      while (pos < json.length()
          && (Character.isDigit(json.charAt(pos))
              || json.charAt(pos) == '.'
              || json.charAt(pos) == 'e'
              || json.charAt(pos) == 'E'
              || json.charAt(pos) == '+'
              || json.charAt(pos) == '-')) {
        pos++;
      }

      String numStr = json.substring(start, pos);
      if (numStr.contains(".") || numStr.contains("e") || numStr.contains("E")) {
        return Double.parseDouble(numStr);
      } else {
        try {
          return Integer.parseInt(numStr);
        } catch (NumberFormatException e) {
          return Long.parseLong(numStr);
        }
      }
    }

    private Boolean parseBoolean() {
      if (json.startsWith("true", pos)) {
        pos += 4;
        return Boolean.TRUE;
      } else if (json.startsWith("false", pos)) {
        pos += 5;
        return Boolean.FALSE;
      } else {
        throw new IllegalArgumentException("Invalid boolean at position " + pos);
      }
    }

    private Object parseNull() {
      if (json.startsWith("null", pos)) {
        pos += 4;
        return null;
      } else {
        throw new IllegalArgumentException("Invalid null at position " + pos);
      }
    }

    private char peek() {
      if (pos >= json.length()) {
        throw new IllegalArgumentException("Unexpected end of JSON");
      }
      return json.charAt(pos);
    }

    private void consume(char expected) {
      char c = peek();
      if (c != expected) {
        throw new IllegalArgumentException(
            "Expected '" + expected + "' but got '" + c + "' at position " + pos);
      }
      pos++;
    }

    private void skipWhitespace() {
      while (pos < json.length() && Character.isWhitespace(json.charAt(pos))) {
        pos++;
      }
    }
  }

  // ==================== Writer ====================

  /** Minimal JSON writer that serializes Java objects to JSON strings. */
  private static class Writer {
    private final StringBuilder sb;

    Writer(StringBuilder sb) {
      this.sb = sb;
    }

    void write(Object obj) {
      if (obj == null) {
        sb.append("null");
      } else if (obj instanceof Map) {
        writeObject((Map<?, ?>) obj);
      } else if (obj instanceof Iterable) {
        writeArray((Iterable<?>) obj);
      } else if (obj instanceof String) {
        writeString((String) obj);
      } else if (obj instanceof Number) {
        writeNumber((Number) obj);
      } else if (obj instanceof Boolean) {
        sb.append(obj.toString());
      } else if (obj.getClass().isArray()) {
        writeArrayPrimitive(obj);
      } else {
        throw new IllegalArgumentException(
            "Unsupported type for JSON serialization: " + obj.getClass().getName());
      }
    }

    private void writeObject(Map<?, ?> map) {
      sb.append('{');
      boolean first = true;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (!first) {
          sb.append(',');
        }
        first = false;

        Object key = entry.getKey();
        if (!(key instanceof String)) {
          throw new IllegalArgumentException(
              "JSON object keys must be strings, got: " + key.getClass().getName());
        }
        writeString((String) key);
        sb.append(':');
        write(entry.getValue());
      }
      sb.append('}');
    }

    private void writeArray(Iterable<?> iterable) {
      sb.append('[');
      boolean first = true;
      for (Object item : iterable) {
        if (!first) {
          sb.append(',');
        }
        first = false;
        write(item);
      }
      sb.append(']');
    }

    private void writeArrayPrimitive(Object array) {
      sb.append('[');
      int length = java.lang.reflect.Array.getLength(array);
      for (int i = 0; i < length; i++) {
        if (i > 0) {
          sb.append(',');
        }
        write(java.lang.reflect.Array.get(array, i));
      }
      sb.append(']');
    }

    private void writeString(String str) {
      sb.append('"');
      for (int i = 0; i < str.length(); i++) {
        char c = str.charAt(i);
        switch (c) {
          case '"':
            sb.append("\\\"");
            break;
          case '\\':
            sb.append("\\\\");
            break;
          case '\b':
            sb.append("\\b");
            break;
          case '\f':
            sb.append("\\f");
            break;
          case '\n':
            sb.append("\\n");
            break;
          case '\r':
            sb.append("\\r");
            break;
          case '\t':
            sb.append("\\t");
            break;
          default:
            if (c < 0x20) {
              sb.append(String.format("\\u%04x", (int) c));
            } else {
              sb.append(c);
            }
        }
      }
      sb.append('"');
    }

    private void writeNumber(Number num) {
      if (num instanceof Double) {
        double d = num.doubleValue();
        if (Double.isInfinite(d) || Double.isNaN(d)) {
          throw new IllegalArgumentException("JSON does not support Infinity or NaN");
        }
      } else if (num instanceof Float) {
        float f = num.floatValue();
        if (Float.isInfinite(f) || Float.isNaN(f)) {
          throw new IllegalArgumentException("JSON does not support Infinity or NaN");
        }
      }
      sb.append(num.toString());
    }
  }
}
