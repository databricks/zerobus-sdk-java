package com.databricks.zerobus.common.json;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for the Json utility class. */
class JsonTest {

  // ==================== Parse Tests ====================

  @Test
  @SuppressWarnings("unchecked")
  void testParseObject() {
    Map<String, Object> result =
        (Map<String, Object>) Json.parse("{\"name\": \"test\", \"count\": 42}");
    assertEquals("test", result.get("name"));
    assertEquals(42, result.get("count"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testParseArray() {
    List<Object> result = (List<Object>) Json.parse("[1, 2, 3]");
    assertEquals(3, result.size());
    assertEquals(1, result.get(0));
    assertEquals(2, result.get(1));
    assertEquals(3, result.get(2));
  }

  @Test
  void testParseString() {
    assertEquals("hello", Json.parse("\"hello\""));
  }

  @Test
  void testParseNumbers() {
    assertEquals(42, Json.parse("42"));
    assertEquals(3.14, Json.parse("3.14"));
    assertEquals(-100, Json.parse("-100"));
    assertEquals(1e10, Json.parse("1e10"));
    assertEquals(1E10, Json.parse("1E10"));
    assertEquals(1.5e-3, Json.parse("1.5e-3"));
    assertEquals(1.5E+3, Json.parse("1.5E+3"));
  }

  @Test
  void testParseLongNumbers() {
    // Numbers exceeding Integer range should parse as Long
    assertEquals(9999999999L, Json.parse("9999999999"));
    assertEquals(-9999999999L, Json.parse("-9999999999"));
    assertEquals(Long.MAX_VALUE, Json.parse(String.valueOf(Long.MAX_VALUE)));
    assertEquals(Long.MIN_VALUE, Json.parse(String.valueOf(Long.MIN_VALUE)));
  }

  @Test
  void testParseBooleans() {
    assertEquals(true, Json.parse("true"));
    assertEquals(false, Json.parse("false"));
  }

  @Test
  void testParseNull() {
    assertNull(Json.parse("null"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testParseNested() {
    String json = "{\"user\": {\"name\": \"Alice\", \"age\": 30}, \"active\": true}";
    Map<String, Object> result = (Map<String, Object>) Json.parse(json);

    Map<String, Object> user = (Map<String, Object>) result.get("user");
    assertEquals("Alice", user.get("name"));
    assertEquals(30, user.get("age"));
    assertEquals(true, result.get("active"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testParseEscapedStrings() {
    Map<String, Object> result =
        (Map<String, Object>) Json.parse("{\"text\": \"hello\\nworld\", \"path\": \"c:\\\\test\"}");
    assertEquals("hello\nworld", result.get("text"));
    assertEquals("c:\\test", result.get("path"));
  }

  @Test
  void testParseAllEscapeCharacters() {
    // Test all JSON escape sequences
    assertEquals("\"", Json.parse("\"\\\"\"")); // \"
    assertEquals("\\", Json.parse("\"\\\\\"")); // \\
    assertEquals("/", Json.parse("\"\\/\"")); // \/
    assertEquals("\b", Json.parse("\"\\b\"")); // \b
    assertEquals("\f", Json.parse("\"\\f\"")); // \f
    assertEquals("\n", Json.parse("\"\\n\"")); // \n
    assertEquals("\r", Json.parse("\"\\r\"")); // \r
    assertEquals("\t", Json.parse("\"\\t\"")); // \t
  }

  @Test
  void testParseUnicodeEscapes() {
    assertEquals("A", Json.parse("\"\\u0041\"")); // Basic ASCII
    assertEquals("\u00E9", Json.parse("\"\\u00e9\"")); // é (lowercase hex)
    assertEquals("\u00E9", Json.parse("\"\\u00E9\"")); // é (uppercase hex)
    assertEquals("\u4E2D", Json.parse("\"\\u4E2D\"")); // 中 (Chinese character)
    assertEquals("Hello\u0020World", Json.parse("\"Hello\\u0020World\"")); // Space as unicode
  }

  @Test
  void testParseEmptyStructures() {
    // Empty object
    @SuppressWarnings("unchecked")
    Map<String, Object> emptyObj = (Map<String, Object>) Json.parse("{}");
    assertTrue(emptyObj.isEmpty());

    // Empty array
    @SuppressWarnings("unchecked")
    List<Object> emptyArr = (List<Object>) Json.parse("[]");
    assertTrue(emptyArr.isEmpty());
  }

  @Test
  void testParseWithWhitespace() {
    // Various whitespace around elements
    assertEquals(42, Json.parse("  42  "));
    assertEquals("test", Json.parse("  \"test\"  "));

    @SuppressWarnings("unchecked")
    Map<String, Object> obj = (Map<String, Object>) Json.parse("  {  \"key\"  :  \"value\"  }  ");
    assertEquals("value", obj.get("key"));

    @SuppressWarnings("unchecked")
    List<Object> arr = (List<Object>) Json.parse("  [  1  ,  2  ,  3  ]  ");
    assertEquals(3, arr.size());
  }

  @Test
  void testParseInvalid() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{invalid}"));
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"key\":}"));
    assertThrows(IllegalArgumentException.class, () -> Json.parse(""));
  }

  @Test
  void testParseInvalidEscapes() {
    // Invalid escape character
    assertThrows(IllegalArgumentException.class, () -> Json.parse("\"\\x\""));
    // Unterminated string escape
    assertThrows(IllegalArgumentException.class, () -> Json.parse("\"test\\"));
    // Invalid unicode escape (too short)
    assertThrows(IllegalArgumentException.class, () -> Json.parse("\"\\u00\""));
    // Unterminated string
    assertThrows(IllegalArgumentException.class, () -> Json.parse("\"unterminated"));
  }

  @Test
  void testParseInvalidStructures() {
    // Missing closing brace
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"key\": \"value\""));
    // Missing closing bracket
    assertThrows(IllegalArgumentException.class, () -> Json.parse("[1, 2, 3"));
    // Invalid boolean
    assertThrows(IllegalArgumentException.class, () -> Json.parse("tru"));
    // Invalid null
    assertThrows(IllegalArgumentException.class, () -> Json.parse("nul"));
    // Unexpected character
    assertThrows(IllegalArgumentException.class, () -> Json.parse("@invalid"));
    // Invalid character after object value (not ',' or '}')
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"key\": \"value\" @}"));
    // Invalid character after array value (not ',' or ']')
    assertThrows(IllegalArgumentException.class, () -> Json.parse("[1 2 3]"));
  }

  // ==================== Stringify Tests ====================

  @Test
  void testStringifyNull() {
    assertEquals("null", Json.stringify(null));
  }

  @Test
  void testStringifyString() {
    assertEquals("\"hello\"", Json.stringify("hello"));
  }

  @Test
  void testStringifyStringWithEscapes() {
    assertEquals("\"hello\\nworld\"", Json.stringify("hello\nworld"));
    assertEquals("\"hello\\tworld\"", Json.stringify("hello\tworld"));
    assertEquals("\"hello\\\"world\"", Json.stringify("hello\"world"));
    assertEquals("\"hello\\\\world\"", Json.stringify("hello\\world"));
  }

  @Test
  void testStringifyAllEscapeCharacters() {
    // All characters that need escaping
    assertEquals("\"\\b\"", Json.stringify("\b")); // backspace
    assertEquals("\"\\f\"", Json.stringify("\f")); // form feed
    assertEquals("\"\\n\"", Json.stringify("\n")); // newline
    assertEquals("\"\\r\"", Json.stringify("\r")); // carriage return
    assertEquals("\"\\t\"", Json.stringify("\t")); // tab
    assertEquals("\"\\\"\"", Json.stringify("\"")); // quote
    assertEquals("\"\\\\\"", Json.stringify("\\")); // backslash
  }

  @Test
  void testStringifyControlCharacters() {
    // Control characters (< 0x20) should be unicode-escaped
    assertEquals("\"\\u0000\"", Json.stringify("\u0000")); // null
    assertEquals("\"\\u0001\"", Json.stringify("\u0001")); // SOH
    assertEquals("\"\\u001f\"", Json.stringify("\u001F")); // unit separator
    // But 0x20 (space) and above should not be escaped
    assertEquals("\" \"", Json.stringify(" ")); // space (0x20)
  }

  @Test
  void testStringifyNumbers() {
    assertEquals("42", Json.stringify(42));
    assertEquals("3.14", Json.stringify(3.14));
    assertEquals("-100", Json.stringify(-100));
    assertEquals("9999999999", Json.stringify(9999999999L));
  }

  @Test
  void testStringifyAllNumberTypes() {
    // Byte
    assertEquals("127", Json.stringify((byte) 127));
    assertEquals("-128", Json.stringify((byte) -128));

    // Short
    assertEquals("32767", Json.stringify((short) 32767));
    assertEquals("-32768", Json.stringify((short) -32768));

    // Integer
    assertEquals("2147483647", Json.stringify(Integer.MAX_VALUE));
    assertEquals("-2147483648", Json.stringify(Integer.MIN_VALUE));

    // Long
    assertEquals("9223372036854775807", Json.stringify(Long.MAX_VALUE));
    assertEquals("-9223372036854775808", Json.stringify(Long.MIN_VALUE));

    // Float
    assertEquals("3.14", Json.stringify(3.14f));
    assertEquals("-1.5", Json.stringify(-1.5f));

    // Double
    assertEquals("3.141592653589793", Json.stringify(3.141592653589793));
  }

  @Test
  void testStringifyBooleans() {
    assertEquals("true", Json.stringify(true));
    assertEquals("false", Json.stringify(false));
  }

  @Test
  void testStringifyList() {
    assertEquals("[1,2,3]", Json.stringify(Arrays.asList(1, 2, 3)));
    assertEquals("[\"a\",\"b\"]", Json.stringify(Arrays.asList("a", "b")));
    assertEquals("[]", Json.stringify(Arrays.asList()));
  }

  @Test
  void testStringifyArray() {
    assertEquals("[1,2,3]", Json.stringify(new int[] {1, 2, 3}));
    assertEquals("[\"a\",\"b\"]", Json.stringify(new String[] {"a", "b"}));
  }

  @Test
  void testStringifyMap() {
    // Use LinkedHashMap to ensure consistent ordering for test
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("name", "Alice");
    map.put("age", 30);
    assertEquals("{\"name\":\"Alice\",\"age\":30}", Json.stringify(map));
  }

  @Test
  void testStringifyEmptyMap() {
    assertEquals("{}", Json.stringify(new HashMap<>()));
  }

  @Test
  void testStringifyNestedMap() {
    Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("city", "NYC");

    Map<String, Object> outer = new LinkedHashMap<>();
    outer.put("name", "Bob");
    outer.put("address", inner);

    assertEquals("{\"name\":\"Bob\",\"address\":{\"city\":\"NYC\"}}", Json.stringify(outer));
  }

  @Test
  void testStringifyMapWithList() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("tags", Arrays.asList("a", "b", "c"));
    assertEquals("{\"tags\":[\"a\",\"b\",\"c\"]}", Json.stringify(map));
  }

  @Test
  void testStringifyMapWithNull() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("value", null);
    assertEquals("{\"value\":null}", Json.stringify(map));
  }

  @Test
  void testStringifyInvalidType() {
    // Custom object without toString representation
    Object custom = new Object() {};
    assertThrows(IllegalArgumentException.class, () -> Json.stringify(custom));
  }

  @Test
  void testStringifyInvalidDoubleInfinity() {
    assertThrows(IllegalArgumentException.class, () -> Json.stringify(Double.POSITIVE_INFINITY));
    assertThrows(IllegalArgumentException.class, () -> Json.stringify(Double.NEGATIVE_INFINITY));
    assertThrows(IllegalArgumentException.class, () -> Json.stringify(Double.NaN));
  }

  @Test
  void testStringifyInvalidFloatInfinity() {
    assertThrows(IllegalArgumentException.class, () -> Json.stringify(Float.POSITIVE_INFINITY));
    assertThrows(IllegalArgumentException.class, () -> Json.stringify(Float.NEGATIVE_INFINITY));
    assertThrows(IllegalArgumentException.class, () -> Json.stringify(Float.NaN));
  }

  @Test
  void testStringifyNonStringKey() {
    Map<Integer, String> map = new HashMap<>();
    map.put(1, "one");
    assertThrows(IllegalArgumentException.class, () -> Json.stringify(map));
  }

  // ==================== Round-trip Tests ====================

  @Test
  @SuppressWarnings("unchecked")
  void testRoundTripSimple() {
    Map<String, Object> original = new LinkedHashMap<>();
    original.put("name", "Test");
    original.put("count", 42);
    original.put("active", true);

    String json = Json.stringify(original);
    Map<String, Object> parsed = (Map<String, Object>) Json.parse(json);

    assertEquals("Test", parsed.get("name"));
    assertEquals(42, parsed.get("count"));
    assertEquals(true, parsed.get("active"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testRoundTripComplex() {
    Map<String, Object> alice = new LinkedHashMap<>();
    alice.put("name", "Alice");
    alice.put("age", 30);

    Map<String, Object> bob = new LinkedHashMap<>();
    bob.put("name", "Bob");
    bob.put("age", 25);

    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("version", 1);

    Map<String, Object> original = new LinkedHashMap<>();
    original.put("users", Arrays.asList(alice, bob));
    original.put("metadata", metadata);

    String json = Json.stringify(original);
    Map<String, Object> parsed = (Map<String, Object>) Json.parse(json);

    List<Object> users = (List<Object>) parsed.get("users");
    assertEquals(2, users.size());

    Map<String, Object> parsedAlice = (Map<String, Object>) users.get(0);
    assertEquals("Alice", parsedAlice.get("name"));
  }
}
