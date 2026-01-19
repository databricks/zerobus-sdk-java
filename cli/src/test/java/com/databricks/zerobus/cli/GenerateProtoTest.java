package com.databricks.zerobus.cli;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.zerobus.common.http.HttpClient;
import com.databricks.zerobus.common.json.Json;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for GenerateProto tool. */
class GenerateProtoTest {

  // ==================== parseArgs Tests ====================

  @Test
  void testParseArgsComplete() {
    String[] args = {
      "--uc-endpoint", "https://endpoint.com",
      "--client-id", "my-client",
      "--client-secret", "my-secret",
      "--table", "cat.sch.tbl",
      "--output", "output.proto",
      "--proto-msg", "MyMessage"
    };

    GenerateProto.Args parsed = GenerateProto.parseArgs(args);

    assertEquals("https://endpoint.com", parsed.ucEndpoint);
    assertEquals("my-client", parsed.clientId);
    assertEquals("my-secret", parsed.clientSecret);
    assertEquals("cat.sch.tbl", parsed.table);
    assertEquals("output.proto", parsed.output);
    assertEquals("MyMessage", parsed.protoMsg);
  }

  @Test
  void testParseArgsWithoutOptionalProtoMsg() {
    String[] args = {
      "--uc-endpoint", "https://endpoint.com",
      "--client-id", "my-client",
      "--client-secret", "my-secret",
      "--table", "cat.sch.tbl",
      "--output", "output.proto"
    };

    GenerateProto.Args parsed = GenerateProto.parseArgs(args);

    assertNull(parsed.protoMsg);
  }

  @Test
  void testParseArgsMissingEndpoint() {
    String[] args = {
      "--client-id", "my-client",
      "--client-secret", "my-secret",
      "--table", "cat.sch.tbl",
      "--output", "output.proto"
    };

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GenerateProto.parseArgs(args));
    assertTrue(ex.getMessage().contains("--uc-endpoint"));
  }

  @Test
  void testParseArgsMissingClientId() {
    String[] args = {
      "--uc-endpoint", "https://endpoint.com",
      "--client-secret", "my-secret",
      "--table", "cat.sch.tbl",
      "--output", "output.proto"
    };

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GenerateProto.parseArgs(args));
    assertTrue(ex.getMessage().contains("--client-id"));
  }

  @Test
  void testParseArgsMissingClientSecret() {
    String[] args = {
      "--uc-endpoint", "https://endpoint.com",
      "--client-id", "my-client",
      "--table", "cat.sch.tbl",
      "--output", "output.proto"
    };

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GenerateProto.parseArgs(args));
    assertTrue(ex.getMessage().contains("--client-secret"));
  }

  @Test
  void testParseArgsMissingTable() {
    String[] args = {
      "--uc-endpoint", "https://endpoint.com",
      "--client-id", "my-client",
      "--client-secret", "my-secret",
      "--output", "output.proto"
    };

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GenerateProto.parseArgs(args));
    assertTrue(ex.getMessage().contains("--table"));
  }

  @Test
  void testParseArgsMissingOutput() {
    String[] args = {
      "--uc-endpoint", "https://endpoint.com",
      "--client-id", "my-client",
      "--client-secret", "my-secret",
      "--table", "cat.sch.tbl"
    };

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GenerateProto.parseArgs(args));
    assertTrue(ex.getMessage().contains("--output"));
  }

  @Test
  void testParseArgsUnknownArgument() {
    String[] args = {
      "--uc-endpoint", "https://endpoint.com",
      "--unknown-arg", "value"
    };

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GenerateProto.parseArgs(args));
    assertTrue(ex.getMessage().contains("Unknown argument"));
  }

  @Test
  void testParseArgsMissingValue() {
    String[] args = {"--uc-endpoint"};

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GenerateProto.parseArgs(args));
    assertTrue(ex.getMessage().contains("Missing value"));
  }

  // ==================== getBasicProtoType Tests ====================

  @Test
  void testGetBasicProtoTypeInt32Types() {
    assertEquals("int32", GenerateProto.getBasicProtoType("INT"));
    assertEquals("int32", GenerateProto.getBasicProtoType("int"));
    assertEquals("int32", GenerateProto.getBasicProtoType("TINYINT"));
    assertEquals("int32", GenerateProto.getBasicProtoType("BYTE"));
    assertEquals("int32", GenerateProto.getBasicProtoType("SMALLINT"));
    assertEquals("int32", GenerateProto.getBasicProtoType("SHORT"));
    assertEquals("int32", GenerateProto.getBasicProtoType("DATE"));
  }

  @Test
  void testGetBasicProtoTypeInt64Types() {
    assertEquals("int64", GenerateProto.getBasicProtoType("BIGINT"));
    assertEquals("int64", GenerateProto.getBasicProtoType("LONG"));
    assertEquals("int64", GenerateProto.getBasicProtoType("TIMESTAMP"));
    assertEquals("int64", GenerateProto.getBasicProtoType("TIMESTAMP_NTZ"));
  }

  @Test
  void testGetBasicProtoTypeStringTypes() {
    assertEquals("string", GenerateProto.getBasicProtoType("STRING"));
    assertEquals("string", GenerateProto.getBasicProtoType("VARIANT"));
  }

  @Test
  void testGetBasicProtoTypeOtherTypes() {
    assertEquals("float", GenerateProto.getBasicProtoType("FLOAT"));
    assertEquals("double", GenerateProto.getBasicProtoType("DOUBLE"));
    assertEquals("bool", GenerateProto.getBasicProtoType("BOOLEAN"));
    assertEquals("bytes", GenerateProto.getBasicProtoType("BINARY"));
  }

  @Test
  void testGetBasicProtoTypeUnknown() {
    assertNull(GenerateProto.getBasicProtoType("UNKNOWN"));
    assertNull(GenerateProto.getBasicProtoType("STRUCT"));
    assertNull(GenerateProto.getBasicProtoType("ARRAY"));
  }

  // ==================== toPascalCase Tests ====================

  @Test
  void testToPascalCaseSimple() {
    assertEquals("FieldName", GenerateProto.toPascalCase("field_name"));
  }

  @Test
  void testToPascalCaseSingleWord() {
    assertEquals("Field", GenerateProto.toPascalCase("field"));
  }

  @Test
  void testToPascalCaseMultipleUnderscores() {
    assertEquals("MyFieldName", GenerateProto.toPascalCase("my_field_name"));
  }

  @Test
  void testToPascalCaseEmpty() {
    assertEquals("", GenerateProto.toPascalCase(""));
  }

  @Test
  void testToPascalCaseNull() {
    assertNull(GenerateProto.toPascalCase(null));
  }

  // ==================== parseStructFields Tests ====================

  @Test
  void testParseStructFieldsSimple() {
    List<String[]> result = GenerateProto.parseStructFields("name:STRING, age:INT");
    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("name", result.get(0)[0]);
    assertEquals("STRING", result.get(0)[1]);
    assertEquals("age", result.get(1)[0]);
    assertEquals("INT", result.get(1)[1]);
  }

  @Test
  void testParseStructFieldsNested() {
    List<String[]> result = GenerateProto.parseStructFields("addr:STRUCT<city:STRING, zip:INT>");
    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals("addr", result.get(0)[0]);
    assertEquals("STRUCT<city:STRING, zip:INT>", result.get(0)[1]);
  }

  @Test
  void testParseStructFieldsInvalidFormat() {
    // Missing colon
    assertNull(GenerateProto.parseStructFields("name STRING"));
  }

  // ==================== getProtoFieldInfo Tests ====================

  @Test
  void testGetProtoFieldInfoBasicNullable() {
    int[] counter = {0};
    GenerateProto.ProtoFieldInfo info =
        GenerateProto.getProtoFieldInfo("field", "STRING", true, counter, 0);
    assertEquals("optional", info.modifier);
    assertEquals("string", info.protoType);
    assertNull(info.nestedDefinition);
  }

  @Test
  void testGetProtoFieldInfoBasicRequired() {
    int[] counter = {0};
    GenerateProto.ProtoFieldInfo info =
        GenerateProto.getProtoFieldInfo("field", "INT", false, counter, 0);
    assertEquals("required", info.modifier);
    assertEquals("int32", info.protoType);
    assertNull(info.nestedDefinition);
  }

  @Test
  void testGetProtoFieldInfoVarchar() {
    int[] counter = {0};
    GenerateProto.ProtoFieldInfo info =
        GenerateProto.getProtoFieldInfo("field", "VARCHAR(255)", true, counter, 0);
    assertEquals("optional", info.modifier);
    assertEquals("string", info.protoType);
    assertNull(info.nestedDefinition);
  }

  @Test
  void testGetProtoFieldInfoArray() {
    int[] counter = {0};
    GenerateProto.ProtoFieldInfo info =
        GenerateProto.getProtoFieldInfo("tags", "ARRAY<STRING>", true, counter, 0);
    assertEquals("repeated", info.modifier);
    assertEquals("string", info.protoType);
    assertNull(info.nestedDefinition);
  }

  @Test
  void testGetProtoFieldInfoMap() {
    int[] counter = {0};
    GenerateProto.ProtoFieldInfo info =
        GenerateProto.getProtoFieldInfo("metadata", "MAP<STRING, INT>", true, counter, 0);
    assertEquals("", info.modifier);
    assertEquals("map<string, int32>", info.protoType);
    assertNull(info.nestedDefinition);
  }

  @Test
  void testGetProtoFieldInfoSimpleStruct() {
    int[] counter = {0};
    GenerateProto.ProtoFieldInfo info =
        GenerateProto.getProtoFieldInfo(
            "location", "STRUCT<lat:DOUBLE, lng:DOUBLE>", true, counter, 0);
    assertEquals("optional", info.modifier);
    assertEquals("Location", info.protoType);
    assertNotNull(info.nestedDefinition);
    assertTrue(info.nestedDefinition.contains("message Location"));
    assertTrue(info.nestedDefinition.contains("optional double lat"));
    assertTrue(info.nestedDefinition.contains("optional double lng"));
  }

  @Test
  void testGetProtoFieldInfoNestedStruct() {
    int[] counter = {0};
    GenerateProto.ProtoFieldInfo info =
        GenerateProto.getProtoFieldInfo(
            "address",
            "STRUCT<city:STRING, location:STRUCT<lat:DOUBLE, lng:DOUBLE>>",
            true,
            counter,
            0);
    assertEquals("optional", info.modifier);
    assertEquals("Address", info.protoType);
    assertNotNull(info.nestedDefinition);
    assertTrue(info.nestedDefinition.contains("message Address"));
    assertTrue(info.nestedDefinition.contains("message Location"));
    assertTrue(info.nestedDefinition.contains("optional string city"));
    assertTrue(info.nestedDefinition.contains("optional double lat"));
  }

  @Test
  void testGetProtoFieldInfoArrayOfStruct() {
    int[] counter = {0};
    GenerateProto.ProtoFieldInfo info =
        GenerateProto.getProtoFieldInfo(
            "addresses", "ARRAY<STRUCT<city:STRING, zip:INT>>", true, counter, 0);
    assertEquals("repeated", info.modifier);
    assertEquals("Addresses", info.protoType);
    assertNotNull(info.nestedDefinition);
    assertTrue(info.nestedDefinition.contains("message Addresses"));
    assertTrue(info.nestedDefinition.contains("optional string city"));
    assertTrue(info.nestedDefinition.contains("optional int32 zip"));
  }

  @Test
  void testGetProtoFieldInfoMapWithStructValue() {
    int[] counter = {0};
    GenerateProto.ProtoFieldInfo info =
        GenerateProto.getProtoFieldInfo(
            "users", "MAP<STRING, STRUCT<name:STRING, age:INT>>", true, counter, 0);
    assertEquals("", info.modifier);
    assertEquals("map<string, Users>", info.protoType);
    assertNotNull(info.nestedDefinition);
    assertTrue(info.nestedDefinition.contains("message Users"));
  }

  @Test
  void testGetProtoFieldInfoStructAsMapKey() {
    int[] counter = {0};
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GenerateProto.getProtoFieldInfo(
                    "field", "MAP<STRUCT<a:INT>, STRING>", true, counter, 0));
    assertTrue(ex.getMessage().contains("Unsupported map key type"));
  }

  @Test
  void testGetProtoFieldInfoNestedArrays() {
    int[] counter = {0};
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> GenerateProto.getProtoFieldInfo("field", "ARRAY<ARRAY<INT>>", true, counter, 0));
    assertTrue(ex.getMessage().contains("Nested arrays are not supported"));
  }

  @Test
  void testGetProtoFieldInfoArrayOfMaps() {
    int[] counter = {0};
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GenerateProto.getProtoFieldInfo(
                    "field", "ARRAY<MAP<STRING, INT>>", true, counter, 0));
    assertTrue(ex.getMessage().contains("Arrays of maps are not supported"));
  }

  @Test
  void testGetProtoFieldInfoMapOfMaps() {
    int[] counter = {0};
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GenerateProto.getProtoFieldInfo(
                    "field", "MAP<STRING, MAP<STRING, INT>>", true, counter, 0));
    assertTrue(ex.getMessage().contains("Maps with map values are not supported"));
  }

  @Test
  void testGetProtoFieldInfoMapOfArrays() {
    int[] counter = {0};
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                GenerateProto.getProtoFieldInfo(
                    "field", "MAP<STRING, ARRAY<INT>>", true, counter, 0));
    assertTrue(ex.getMessage().contains("Maps with array values are not supported"));
  }

  @Test
  void testGetProtoFieldInfoUnsupportedType() {
    int[] counter = {0};
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> GenerateProto.getProtoFieldInfo("field", "UNSUPPORTED_TYPE", true, counter, 0));
    assertTrue(ex.getMessage().contains("Unsupported column type"));
  }

  @Test
  void testGetProtoFieldInfoMaxNestingDepthExceeded() {
    int[] counter = {0};
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> GenerateProto.getProtoFieldInfo("field", "STRING", true, counter, 101));
    assertTrue(ex.getMessage().contains("Nesting level exceeds maximum depth"));
  }

  @Test
  void testGetProtoFieldInfoStructWithMapField() {
    int[] counter = {0};
    GenerateProto.ProtoFieldInfo info =
        GenerateProto.getProtoFieldInfo(
            "config", "STRUCT<name:STRING, metadata:MAP<STRING, INT>>", true, counter, 0);
    assertEquals("optional", info.modifier);
    assertEquals("Config", info.protoType);
    assertNotNull(info.nestedDefinition);
    assertTrue(info.nestedDefinition.contains("message Config"));
    assertTrue(info.nestedDefinition.contains("optional string name"));
    assertTrue(info.nestedDefinition.contains("map<string, int32> metadata"));
  }

  // ==================== generateProtoContent Tests ====================

  @Test
  void testGenerateProtoContentWithStruct() throws IOException {
    List<Map<String, Object>> columns = new ArrayList<>();
    columns.add(createColumn("id", "INT", false));
    columns.add(createColumn("location", "STRUCT<lat:DOUBLE, lng:DOUBLE>", true));

    StringWriter writer = new StringWriter();
    GenerateProto.generateProtoContent("MessageWithStruct", columns, writer);
    String proto = writer.toString();

    assertTrue(proto.contains("syntax = \"proto3\";"));
    assertTrue(proto.contains("message MessageWithStruct {"));
    assertTrue(proto.contains("message Location {"));
    assertTrue(proto.contains("optional double lat = 1;"));
    assertTrue(proto.contains("optional double lng = 2;"));
    assertTrue(proto.contains("required int32 id = 1;"));
    assertTrue(proto.contains("optional Location location = 2;"));
  }

  @Test
  void testGenerateProtoContentWithNestedStruct() throws IOException {
    List<Map<String, Object>> columns = new ArrayList<>();
    columns.add(
        createColumn(
            "address", "STRUCT<city:STRING, coords:STRUCT<lat:DOUBLE, lng:DOUBLE>>", true));

    StringWriter writer = new StringWriter();
    GenerateProto.generateProtoContent("NestedMessage", columns, writer);
    String proto = writer.toString();

    assertTrue(proto.contains("message NestedMessage {"));
    assertTrue(proto.contains("message Address {"));
    assertTrue(proto.contains("message Coords {"));
    assertTrue(proto.contains("optional double lat"));
    assertTrue(proto.contains("optional double lng"));
    assertTrue(proto.contains("optional string city"));
    assertTrue(proto.contains("optional Coords coords"));
    assertTrue(proto.contains("optional Address address"));
  }

  @Test
  void testGenerateProtoContentWithArrayOfStruct() throws IOException {
    List<Map<String, Object>> columns = new ArrayList<>();
    columns.add(createColumn("people", "ARRAY<STRUCT<name:STRING, age:INT>>", true));

    StringWriter writer = new StringWriter();
    GenerateProto.generateProtoContent("ArrayOfStructMessage", columns, writer);
    String proto = writer.toString();

    assertTrue(proto.contains("message ArrayOfStructMessage {"));
    assertTrue(proto.contains("message People {"));
    assertTrue(proto.contains("optional string name"));
    assertTrue(proto.contains("optional int32 age"));
    assertTrue(proto.contains("repeated People people"));
  }

  // ==================== extractColumns Tests ====================

  @Test
  void testExtractColumnsValid() {
    Map<String, Object> tableInfo = new HashMap<>();
    List<Map<String, Object>> columns = new ArrayList<>();
    columns.add(createColumn("id", "INT", false));
    columns.add(createColumn("name", "STRING", true));
    tableInfo.put("columns", columns);

    List<Map<String, Object>> result = GenerateProto.extractColumns(tableInfo);

    assertEquals(2, result.size());
    assertEquals("id", result.get(0).get("name"));
    assertEquals("name", result.get(1).get("name"));
  }

  @Test
  void testExtractColumnsNoColumns() {
    Map<String, Object> tableInfo = new HashMap<>();

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GenerateProto.extractColumns(tableInfo));
    assertTrue(ex.getMessage().contains("No columns found"));
  }

  // ==================== generateProtoContent Tests ====================

  @Test
  void testGenerateProtoContentBasic() throws IOException {
    List<Map<String, Object>> columns = new ArrayList<>();
    columns.add(createColumn("id", "INT", false));
    columns.add(createColumn("name", "STRING", true));
    columns.add(createColumn("value", "DOUBLE", true));

    StringWriter writer = new StringWriter();
    GenerateProto.generateProtoContent("TestMessage", columns, writer);
    String proto = writer.toString();

    assertTrue(proto.contains("syntax = \"proto3\";"));
    assertTrue(proto.contains("message TestMessage {"));
    assertTrue(proto.contains("required int32 id = 1;"));
    assertTrue(proto.contains("optional string name = 2;"));
    assertTrue(proto.contains("optional double value = 3;"));
    assertTrue(proto.contains("}"));
  }

  @Test
  void testGenerateProtoContentWithArray() throws IOException {
    List<Map<String, Object>> columns = new ArrayList<>();
    columns.add(createColumn("tags", "ARRAY<STRING>", true));

    StringWriter writer = new StringWriter();
    GenerateProto.generateProtoContent("ArrayMessage", columns, writer);
    String proto = writer.toString();

    assertTrue(proto.contains("repeated string tags = 1;"));
  }

  @Test
  void testGenerateProtoContentWithMap() throws IOException {
    List<Map<String, Object>> columns = new ArrayList<>();
    columns.add(createColumn("metadata", "MAP<STRING, INT>", true));

    StringWriter writer = new StringWriter();
    GenerateProto.generateProtoContent("MapMessage", columns, writer);
    String proto = writer.toString();

    assertTrue(proto.contains("map<string, int32> metadata = 1;"));
  }

  @Test
  void testGenerateProtoContentAllTypes() throws IOException {
    List<Map<String, Object>> columns = new ArrayList<>();
    columns.add(createColumn("tiny", "TINYINT", true));
    columns.add(createColumn("small", "SMALLINT", true));
    columns.add(createColumn("regular", "INT", true));
    columns.add(createColumn("big", "BIGINT", true));
    columns.add(createColumn("text", "STRING", true));
    columns.add(createColumn("decimal", "FLOAT", true));
    columns.add(createColumn("precise", "DOUBLE", true));
    columns.add(createColumn("flag", "BOOLEAN", true));
    columns.add(createColumn("data", "BINARY", true));
    columns.add(createColumn("day", "DATE", true));
    columns.add(createColumn("moment", "TIMESTAMP", true));
    columns.add(createColumn("local_moment", "TIMESTAMP_NTZ", true));
    columns.add(createColumn("json_data", "VARIANT", true));

    StringWriter writer = new StringWriter();
    GenerateProto.generateProtoContent("AllTypesMessage", columns, writer);
    String proto = writer.toString();

    assertTrue(proto.contains("optional int32 tiny = 1;"));
    assertTrue(proto.contains("optional int32 small = 2;"));
    assertTrue(proto.contains("optional int32 regular = 3;"));
    assertTrue(proto.contains("optional int64 big = 4;"));
    assertTrue(proto.contains("optional string text = 5;"));
    assertTrue(proto.contains("optional float decimal = 6;"));
    assertTrue(proto.contains("optional double precise = 7;"));
    assertTrue(proto.contains("optional bool flag = 8;"));
    assertTrue(proto.contains("optional bytes data = 9;"));
    assertTrue(proto.contains("optional int32 day = 10;"));
    assertTrue(proto.contains("optional int64 moment = 11;"));
    assertTrue(proto.contains("optional int64 local_moment = 12;"));
    assertTrue(proto.contains("optional string json_data = 13;"));
  }

  // ==================== parseJson Tests ====================

  @Test
  @SuppressWarnings("unchecked")
  void testParseJsonObject() {
    String json = "{\"name\": \"test\", \"count\": 42}";
    Map<String, Object> result = (Map<String, Object>) Json.parse(json);

    assertEquals("test", result.get("name"));
    assertEquals(42, result.get("count"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testParseJsonArray() {
    String json = "[1, 2, 3]";
    List<Object> result = (List<Object>) Json.parse(json);

    assertEquals(3, result.size());
    assertEquals(1, result.get(0));
    assertEquals(2, result.get(1));
    assertEquals(3, result.get(2));
  }

  @Test
  void testParseJsonString() {
    String json = "\"hello\"";
    String result = (String) Json.parse(json);
    assertEquals("hello", result);
  }

  @Test
  void testParseJsonNumber() {
    assertEquals(42, Json.parse("42"));
    assertEquals(3.14, Json.parse("3.14"));
    assertEquals(-100, Json.parse("-100"));
  }

  @Test
  void testParseJsonBoolean() {
    assertEquals(true, Json.parse("true"));
    assertEquals(false, Json.parse("false"));
  }

  @Test
  void testParseJsonNull() {
    assertNull(Json.parse("null"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testParseJsonNested() {
    String json = "{\"user\": {\"name\": \"Alice\", \"age\": 30}, \"active\": true}";
    Map<String, Object> result = (Map<String, Object>) Json.parse(json);

    Map<String, Object> user = (Map<String, Object>) result.get("user");
    assertEquals("Alice", user.get("name"));
    assertEquals(30, user.get("age"));
    assertEquals(true, result.get("active"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testParseJsonEscapedStrings() {
    String json = "{\"text\": \"hello\\nworld\", \"path\": \"c:\\\\test\"}";
    Map<String, Object> result = (Map<String, Object>) Json.parse(json);

    assertEquals("hello\nworld", result.get("text"));
    assertEquals("c:\\test", result.get("path"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testParseJsonUnicode() {
    String json = "{\"emoji\": \"\\u0048\\u0065\\u006C\\u006C\\u006F\"}";
    Map<String, Object> result = (Map<String, Object>) Json.parse(json);
    assertEquals("Hello", result.get("emoji"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testParseJsonEmptyObject() {
    String json = "{}";
    Map<String, Object> result = (Map<String, Object>) Json.parse(json);
    assertTrue(result.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testParseJsonEmptyArray() {
    String json = "[]";
    List<Object> result = (List<Object>) Json.parse(json);
    assertTrue(result.isEmpty());
  }

  @Test
  void testParseJsonInvalid() {
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{invalid}"));
    assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"key\":}"));
    assertThrows(IllegalArgumentException.class, () -> Json.parse(""));
  }

  // ==================== HTTP Integration Tests ====================

  @Test
  void testGetOAuthTokenSuccess() throws Exception {
    MockHttpClient mockClient = new MockHttpClient();
    mockClient.setResponse(
        new HttpClient.HttpResponse(200, "{\"access_token\": \"my-token\"}", null));

    String token =
        GenerateProto.getOAuthToken("https://endpoint.com", "client", "secret", mockClient);

    assertEquals("my-token", token);
    assertEquals("https://endpoint.com/oidc/v1/token", mockClient.lastUrl);
  }

  @Test
  void testGetOAuthTokenFailure() {
    MockHttpClient mockClient = new MockHttpClient();
    mockClient.setResponse(new HttpClient.HttpResponse(401, null, "Unauthorized"));

    IOException ex =
        assertThrows(
            IOException.class,
            () ->
                GenerateProto.getOAuthToken(
                    "https://endpoint.com", "client", "secret", mockClient));

    assertTrue(ex.getMessage().contains("401"));
  }

  @Test
  void testGetOAuthTokenMissingAccessToken() {
    MockHttpClient mockClient = new MockHttpClient();
    mockClient.setResponse(new HttpClient.HttpResponse(200, "{\"error\": \"invalid\"}", null));

    IOException ex =
        assertThrows(
            IOException.class,
            () ->
                GenerateProto.getOAuthToken(
                    "https://endpoint.com", "client", "secret", mockClient));

    assertTrue(ex.getMessage().contains("No access token"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testFetchTableInfoSuccess() throws Exception {
    MockHttpClient mockClient = new MockHttpClient();
    String tableJson =
        "{\"name\": \"test_table\", \"columns\": [{\"name\": \"id\", \"type_text\": \"INT\", \"nullable\": false}]}";
    mockClient.setResponse(new HttpClient.HttpResponse(200, tableJson, null));

    Map<String, Object> result =
        GenerateProto.fetchTableInfo("https://endpoint.com", "token", "cat.sch.tbl", mockClient);

    assertEquals("test_table", result.get("name"));
    List<Map<String, Object>> columns = (List<Map<String, Object>>) result.get("columns");
    assertEquals(1, columns.size());
    assertEquals("id", columns.get(0).get("name"));
  }

  @Test
  void testFetchTableInfoFailure() {
    MockHttpClient mockClient = new MockHttpClient();
    mockClient.setResponse(new HttpClient.HttpResponse(404, null, "Table not found"));

    IOException ex =
        assertThrows(
            IOException.class,
            () ->
                GenerateProto.fetchTableInfo(
                    "https://endpoint.com", "token", "cat.sch.tbl", mockClient));

    assertTrue(ex.getMessage().contains("404"));
  }

  // ==================== Helper Methods ====================

  private Map<String, Object> createColumn(String name, String typeText, boolean nullable) {
    Map<String, Object> col = new HashMap<>();
    col.put("name", name);
    col.put("type_text", typeText);
    col.put("nullable", nullable);
    return col;
  }

  /** Mock HttpClient for testing. */
  private static class MockHttpClient implements HttpClient {
    private HttpClient.HttpResponse response;
    private String lastUrl;

    void setResponse(HttpClient.HttpResponse response) {
      this.response = response;
    }

    @Override
    public HttpClient.HttpResponse post(String url, String formData, Map<String, String> headers) {
      this.lastUrl = url;
      return response;
    }

    @Override
    public HttpClient.HttpResponse get(String url, Map<String, String> headers) {
      this.lastUrl = url;
      return response;
    }
  }
}
