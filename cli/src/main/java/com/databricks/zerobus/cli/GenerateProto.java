package com.databricks.zerobus.cli;

import com.databricks.zerobus.common.http.DefaultHttpClient;
import com.databricks.zerobus.common.http.HttpClient;
import com.databricks.zerobus.common.http.RetryingHttpClient;
import com.databricks.zerobus.common.json.Json;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generate proto3 file from Unity Catalog table schema.
 *
 * <p>This tool fetches table schema from Unity Catalog and generates a corresponding proto3
 * definition file. It supports all Delta data types and maps them to appropriate Protocol Buffer
 * types.
 *
 * <p>Usage: java GenerateProto --uc-endpoint &lt;endpoint&gt; --client-id &lt;id&gt;
 * --client-secret &lt;secret&gt; --table &lt;catalog.schema.table&gt; --output &lt;output.proto&gt;
 * [--proto-msg &lt;message_name&gt;]
 *
 * <p>Type mappings: INT -&gt; int32 STRING -&gt; string FLOAT -&gt; float LONG/BIGINT -&gt; int64
 * SHORT/SMALLINT -&gt; int32 DOUBLE -&gt; double BOOLEAN -&gt; bool BINARY -&gt; bytes DATE -&gt;
 * int32 TIMESTAMP -&gt; int64 ARRAY&lt;type&gt; -&gt; repeated type MAP&lt;key_type, value_type&gt;
 * -&gt; map&lt;key_type, value_type&gt;
 */
public class GenerateProto {

  // Proto field modifiers
  private static final String OPTIONAL = "optional";
  private static final String REQUIRED = "required";
  private static final String REPEATED = "repeated";

  // Proto types
  private static final String PROTO_INT32 = "int32";
  private static final String PROTO_INT64 = "int64";
  private static final String PROTO_STRING = "string";
  private static final String PROTO_FLOAT = "float";
  private static final String PROTO_DOUBLE = "double";
  private static final String PROTO_BOOL = "bool";
  private static final String PROTO_BYTES = "bytes";

  // Compiled regex patterns for type matching
  private static final Pattern ARRAY_PATTERN =
      Pattern.compile("^ARRAY<(.+)>$", Pattern.CASE_INSENSITIVE);
  private static final Pattern MAP_PATTERN =
      Pattern.compile("^MAP<(.+)>$", Pattern.CASE_INSENSITIVE);
  private static final Pattern STRUCT_PATTERN =
      Pattern.compile("^STRUCT<(.+)>$", Pattern.CASE_INSENSITIVE);
  private static final Pattern VARCHAR_PATTERN =
      Pattern.compile("^VARCHAR(\\(\\d+\\))?$", Pattern.CASE_INSENSITIVE);

  private static final String USAGE =
      "Usage: java GenerateProto \n"
          + "  --uc-endpoint <endpoint>      Unity Catalog endpoint URL\n"
          + "  --client-id <id>              OAuth client ID\n"
          + "  --client-secret <secret>      OAuth client secret\n"
          + "  --table <catalog.schema.table> Full table name\n"
          + "  --output <output.proto>       Output path for proto file\n"
          + "  [--proto-msg <message_name>]  Name of protobuf message (defaults to table name)\n"
          + "\n"
          + "Examples:\n"
          + "  java GenerateProto \\\n"
          + "    --uc-endpoint \"https://your-workspace.cloud.databricks.com\" \\\n"
          + "    --client-id \"your-client-id\" \\\n"
          + "    --client-secret \"your-client-secret\" \\\n"
          + "    --table \"catalog.schema.table_name\" \\\n"
          + "    --proto-msg \"TableMessage\" \\\n"
          + "    --output \"output.proto\"\n"
          + "\n"
          + "Type mappings:\n"
          + "  Delta            -> Proto2\n"
          + "  TINYINT/BYTE     -> int32\n"
          + "  SMALLINT/SHORT   -> int32\n"
          + "  INT              -> int32\n"
          + "  BIGINT/LONG      -> int64\n"
          + "  STRING           -> string\n"
          + "  FLOAT            -> float\n"
          + "  DOUBLE           -> double\n"
          + "  BOOLEAN          -> bool\n"
          + "  BINARY           -> bytes\n"
          + "  DATE             -> int32\n"
          + "  TIMESTAMP        -> int64\n"
          + "  TIMESTAMP_NTZ    -> int64\n"
          + "  VARIANT          -> string (unshredded, JSON string)\n"
          + "  ARRAY<type>      -> repeated type\n"
          + "  MAP<key_type, value_type> -> map<key_type, value_type>\n"
          + "  STRUCT<fields>   -> nested message\n";

  public static void main(String[] args) {
    try {
      Args parsedArgs = parseArgs(args);
      run(parsedArgs, new RetryingHttpClient(DefaultHttpClient.INSTANCE));
      System.out.println("Successfully generated proto file at: " + parsedArgs.output);
      System.exit(0);
    } catch (IllegalArgumentException e) {
      System.err.println("Error: " + e.getMessage());
      System.err.println();
      System.err.println(USAGE);
      System.exit(1);
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Runs the proto generation with the given arguments and HTTP client.
   *
   * @param args The parsed command line arguments
   * @param httpClient The HTTP client to use for API calls
   * @throws Exception if generation fails
   */
  static void run(Args args, HttpClient httpClient) throws Exception {
    String token = getOAuthToken(args.ucEndpoint, args.clientId, args.clientSecret, httpClient);

    Map<String, Object> tableInfo = fetchTableInfo(args.ucEndpoint, token, args.table, httpClient);

    List<Map<String, Object>> columns = extractColumns(tableInfo);

    String messageName = args.protoMsg != null ? args.protoMsg : args.table.split("\\.")[2];

    try (FileWriter writer = new FileWriter(args.output)) {
      generateProtoContent(messageName, columns, writer);
    }
  }

  /**
   * Parses command line arguments.
   *
   * @param args The command line arguments
   * @return Parsed arguments
   * @throws IllegalArgumentException if required arguments are missing or invalid
   */
  static Args parseArgs(String[] args) {
    Args result = new Args();

    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("--help") || arg.equals("-h")) {
        System.out.println(USAGE);
        System.exit(0);
      }
      if (arg.startsWith("--")) {
        String key = arg.substring(2);
        if (i + 1 >= args.length) {
          throw new IllegalArgumentException("Missing value for argument: " + arg);
        }
        String value = args[++i];

        switch (key) {
          case "uc-endpoint":
            result.ucEndpoint = value;
            break;
          case "client-id":
            result.clientId = value;
            break;
          case "client-secret":
            result.clientSecret = value;
            break;
          case "table":
            result.table = value;
            break;
          case "output":
            result.output = value;
            break;
          case "proto-msg":
            result.protoMsg = value;
            break;
          default:
            throw new IllegalArgumentException("Unknown argument: " + arg);
        }
      }
    }

    // Validate required arguments
    if (result.ucEndpoint == null) {
      throw new IllegalArgumentException("Missing required argument: --uc-endpoint");
    }
    if (result.clientId == null) {
      throw new IllegalArgumentException("Missing required argument: --client-id");
    }
    if (result.clientSecret == null) {
      throw new IllegalArgumentException("Missing required argument: --client-secret");
    }
    if (result.table == null) {
      throw new IllegalArgumentException("Missing required argument: --table");
    }
    if (result.output == null) {
      throw new IllegalArgumentException("Missing required argument: --output");
    }

    return result;
  }

  /**
   * Obtains an OAuth token using client credentials flow.
   *
   * @param ucEndpoint The Unity Catalog endpoint URL
   * @param clientId The OAuth client ID
   * @param clientSecret The OAuth client secret
   * @param httpClient The HTTP client to use
   * @return The OAuth access token (JWT)
   * @throws Exception if the token request fails
   */
  static String getOAuthToken(
      String ucEndpoint, String clientId, String clientSecret, HttpClient httpClient)
      throws Exception {
    String url = ucEndpoint + "/oidc/v1/token";
    String formData = "grant_type=client_credentials&scope=all-apis";

    String credentials =
        Base64.getEncoder()
            .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Basic " + credentials);

    HttpClient.HttpResponse response = httpClient.post(url, formData, headers);

    if (!response.isSuccess()) {
      String errorBody =
          response.getErrorBody() != null ? response.getErrorBody() : "No error details available";
      throw new IOException(
          "OAuth request failed with status " + response.getStatusCode() + ": " + errorBody);
    }

    Pattern accessTokenPattern = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
    Matcher matcher = accessTokenPattern.matcher(response.getBody());

    if (matcher.find()) {
      return matcher.group(1);
    } else {
      throw new IOException("No access token received from OAuth response");
    }
  }

  /**
   * Fetch table information from Unity Catalog.
   *
   * @param endpoint Base URL of the Unity Catalog endpoint
   * @param token Authentication token
   * @param table Table identifier (catalog.schema.table)
   * @param httpClient The HTTP client to use
   * @return The parsed table information as a Map
   * @throws Exception If the HTTP request fails
   */
  @SuppressWarnings("unchecked")
  static Map<String, Object> fetchTableInfo(
      String endpoint, String token, String table, HttpClient httpClient) throws Exception {
    String encodedTable = URLEncoder.encode(table, "UTF-8");
    String url = endpoint + "/api/2.1/unity-catalog/tables/" + encodedTable;

    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Bearer " + token);
    headers.put("Content-Type", "application/json");

    HttpClient.HttpResponse response = httpClient.get(url, headers);

    if (!response.isSuccess()) {
      String errorBody =
          response.getErrorBody() != null ? response.getErrorBody() : "No error details available";
      throw new IOException(
          "Failed to fetch table info with status " + response.getStatusCode() + ": " + errorBody);
    }

    return (Map<String, Object>) Json.parse(response.getBody());
  }

  /**
   * Extract column information from the table schema.
   *
   * @param tableInfo Raw table information from Unity Catalog
   * @return List of column information maps
   * @throws IllegalArgumentException If the expected schema structure is not found
   */
  @SuppressWarnings("unchecked")
  static List<Map<String, Object>> extractColumns(Map<String, Object> tableInfo) {
    if (!tableInfo.containsKey("columns")) {
      throw new IllegalArgumentException("No columns found in table info");
    }
    return (List<Map<String, Object>>) tableInfo.get("columns");
  }

  /**
   * Get basic proto type mapping for simple types.
   *
   * @param type The Unity Catalog type
   * @return The proto type or null if not a basic type
   */
  static String getBasicProtoType(String type) {
    String upperType = type.trim().toUpperCase();
    switch (upperType) {
      case "TINYINT":
      case "BYTE":
      case "SMALLINT":
      case "SHORT":
      case "INT":
      case "DATE":
        return PROTO_INT32;
      case "BIGINT":
      case "LONG":
      case "TIMESTAMP":
      case "TIMESTAMP_NTZ":
        return PROTO_INT64;
      case "STRING":
      case "VARIANT":
        return PROTO_STRING;
      case "FLOAT":
        return PROTO_FLOAT;
      case "DOUBLE":
        return PROTO_DOUBLE;
      case "BOOLEAN":
        return PROTO_BOOL;
      case "BINARY":
        return PROTO_BYTES;
      default:
        if (VARCHAR_PATTERN.matcher(upperType).matches()) {
          return PROTO_STRING;
        }
        return null;
    }
  }

  /**
   * Convert a snake_case string to PascalCase.
   *
   * @param s The string to convert (e.g., "field_name")
   * @return The string in PascalCase (e.g., "FieldName")
   */
  static String toPascalCase(String s) {
    if (s == null || s.isEmpty()) {
      return s;
    }
    StringBuilder result = new StringBuilder();
    for (String word : s.split("_")) {
      if (!word.isEmpty()) {
        result.append(Character.toUpperCase(word.charAt(0)));
        if (word.length() > 1) {
          result.append(word.substring(1));
        }
      }
    }
    return result.toString();
  }

  /**
   * Parse struct fields from the inner content of a STRUCT type.
   *
   * @param inner The inner content of STRUCT (e.g., "field1:STRING, field2:INT")
   * @return List of field name/type pairs, or null if parsing fails
   */
  static List<String[]> parseStructFields(String inner) {
    List<String> fields = new ArrayList<>();
    int depth = 0;
    StringBuilder current = new StringBuilder();

    for (char c : inner.toCharArray()) {
      if (c == '<') {
        depth++;
        current.append(c);
      } else if (c == '>') {
        depth--;
        current.append(c);
      } else if (c == ',' && depth == 0) {
        fields.add(current.toString().trim());
        current = new StringBuilder();
      } else {
        current.append(c);
      }
    }

    String lastField = current.toString().trim();
    if (!lastField.isEmpty()) {
      fields.add(lastField);
    }

    List<String[]> result = new ArrayList<>();
    for (String field : fields) {
      int colonIndex = field.indexOf(':');
      if (colonIndex == -1) {
        return null;
      }
      String fieldName = field.substring(0, colonIndex).trim();
      String fieldType = field.substring(colonIndex + 1).trim();
      result.add(new String[] {fieldName, fieldType});
    }

    return result.isEmpty() ? null : result;
  }

  /** Holds proto field information including any nested message definitions. */
  static class ProtoFieldInfo {
    final String modifier;
    final String protoType;
    final String nestedDefinition;

    ProtoFieldInfo(String modifier, String protoType, String nestedDefinition) {
      this.modifier = modifier;
      this.protoType = protoType;
      this.nestedDefinition = nestedDefinition;
    }
  }

  /** Maximum nesting depth for STRUCT types to prevent infinite recursion. */
  private static final int MAX_NESTING_DEPTH = 100;

  /**
   * Map Unity Catalog column types to proto3 field information.
   *
   * <p>Supports all Unity Catalog types including nested STRUCTs, ARRAYs, and MAPs.
   *
   * @param fieldName The field name (used for naming struct messages)
   * @param columnType The Unity Catalog column type
   * @param nullable Whether the column is nullable
   * @param structCounter Counter for generating unique struct names (mutable)
   * @param level Current nesting level (for recursion depth tracking)
   * @return ProtoFieldInfo with modifier, type, and optional nested definition
   * @throws IllegalArgumentException If the column type is not supported or nesting is too deep
   */
  static ProtoFieldInfo getProtoFieldInfo(
      String fieldName, String columnType, boolean nullable, int[] structCounter, int level) {

    if (level > MAX_NESTING_DEPTH) {
      throw new IllegalArgumentException(
          "Nesting level exceeds maximum depth of " + MAX_NESTING_DEPTH);
    }

    String trimmedType = columnType.trim();

    String protoType = getBasicProtoType(trimmedType);
    if (protoType != null) {
      return new ProtoFieldInfo(nullable ? OPTIONAL : REQUIRED, protoType, null);
    }

    Matcher arrayMatcher = ARRAY_PATTERN.matcher(trimmedType);
    if (arrayMatcher.matches()) {
      String elementType = arrayMatcher.group(1).trim();

      if (ARRAY_PATTERN.matcher(elementType).matches()) {
        throw new IllegalArgumentException("Nested arrays are not supported: ARRAY<ARRAY<...>>");
      }

      if (MAP_PATTERN.matcher(elementType).matches()) {
        throw new IllegalArgumentException("Arrays of maps are not supported: ARRAY<MAP<...>>");
      }

      ProtoFieldInfo elementInfo =
          getProtoFieldInfo(fieldName, elementType, false, structCounter, level + 1);
      return new ProtoFieldInfo(REPEATED, elementInfo.protoType, elementInfo.nestedDefinition);
    }

    Matcher mapMatcher = MAP_PATTERN.matcher(trimmedType);
    if (mapMatcher.matches()) {
      String inner = mapMatcher.group(1);

      int depth = 0;
      int splitIndex = -1;
      for (int i = 0; i < inner.length(); i++) {
        char c = inner.charAt(i);
        if (c == '<') depth++;
        else if (c == '>') depth--;
        else if (c == ',' && depth == 0) {
          splitIndex = i;
          break;
        }
      }

      if (splitIndex == -1) {
        throw new IllegalArgumentException("Invalid map type: " + columnType);
      }

      String keyType = inner.substring(0, splitIndex).trim();
      String valueType = inner.substring(splitIndex + 1).trim();

      ProtoFieldInfo keyInfo =
          getProtoFieldInfo(fieldName, keyType, false, structCounter, level + 1);
      if (keyInfo.nestedDefinition != null) {
        throw new IllegalArgumentException("Unsupported map key type: " + keyType);
      }

      if (MAP_PATTERN.matcher(valueType).matches()) {
        throw new IllegalArgumentException(
            "Maps with map values are not supported: MAP<..., MAP<...>>");
      }

      if (ARRAY_PATTERN.matcher(valueType).matches()) {
        throw new IllegalArgumentException(
            "Maps with array values are not supported: MAP<..., ARRAY<...>>");
      }

      ProtoFieldInfo valueInfo =
          getProtoFieldInfo(fieldName, valueType, false, structCounter, level + 1);

      String mapType = "map<" + keyInfo.protoType + ", " + valueInfo.protoType + ">";
      return new ProtoFieldInfo("", mapType, valueInfo.nestedDefinition);
    }

    Matcher structMatcher = STRUCT_PATTERN.matcher(trimmedType);
    if (structMatcher.matches()) {
      List<String[]> structFields = parseStructFields(structMatcher.group(1));
      structCounter[0]++;
      String baseName = toPascalCase(fieldName);
      String structName =
          (baseName != null && !baseName.isEmpty()) ? baseName : "Struct" + structCounter[0];

      String indent = repeat("    ", level);
      String innerIndent = repeat("    ", level + 1);

      StringBuilder structDef = new StringBuilder();
      structDef.append(indent).append("message ").append(structName).append(" {\n");

      int fieldNumber = 1;
      for (String[] fieldPair : structFields) {
        String fname = fieldPair[0];
        String ftype = fieldPair[1];

        ProtoFieldInfo fieldInfo = getProtoFieldInfo(fname, ftype, true, structCounter, level + 1);

        if (fieldInfo.nestedDefinition != null) {
          structDef.append(fieldInfo.nestedDefinition).append("\n\n");
        }

        if (fieldInfo.modifier.isEmpty()) {
          structDef
              .append(innerIndent)
              .append(fieldInfo.protoType)
              .append(" ")
              .append(fname)
              .append(" = ")
              .append(fieldNumber)
              .append(";\n");
        } else {
          structDef
              .append(innerIndent)
              .append(fieldInfo.modifier)
              .append(" ")
              .append(fieldInfo.protoType)
              .append(" ")
              .append(fname)
              .append(" = ")
              .append(fieldNumber)
              .append(";\n");
        }
        fieldNumber++;
      }

      structDef.append(indent).append("}");

      return new ProtoFieldInfo(nullable ? OPTIONAL : REQUIRED, structName, structDef.toString());
    }

    throw new IllegalArgumentException("Unsupported column type: " + columnType);
  }

  /** Repeat a string n times. */
  private static String repeat(String s, int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      sb.append(s);
    }
    return sb.toString();
  }

  /**
   * Generate proto3 content from the column information.
   *
   * @param messageName Name of the protobuf message
   * @param columns List of column information maps
   * @param writer Writer to write the proto content to
   * @throws IOException If writing fails
   */
  static void generateProtoContent(
      String messageName, List<Map<String, Object>> columns, Writer writer) throws IOException {
    writer.write("syntax = \"proto3\";\n");
    writer.write("\n");
    writer.write("message " + messageName + " {\n");

    int[] structCounter = {0};
    int fieldNumber = 1;
    for (Map<String, Object> col : columns) {
      String fieldName = (String) col.get("name");
      String typeText = (String) col.get("type_text");
      boolean nullable = (Boolean) col.get("nullable");

      ProtoFieldInfo fieldInfo = getProtoFieldInfo(fieldName, typeText, nullable, structCounter, 1);

      if (fieldInfo.nestedDefinition != null) {
        writer.write(fieldInfo.nestedDefinition);
        writer.write("\n\n");
      }

      if (fieldInfo.modifier.isEmpty()) {
        writer.write("    " + fieldInfo.protoType + " " + fieldName + " = " + fieldNumber + ";\n");
      } else {
        writer.write(
            "    "
                + fieldInfo.modifier
                + " "
                + fieldInfo.protoType
                + " "
                + fieldName
                + " = "
                + fieldNumber
                + ";\n");
      }
      fieldNumber++;
    }

    writer.write("}\n");
  }

  /** Command-line arguments holder. */
  static class Args {
    String ucEndpoint;
    String clientId;
    String clientSecret;
    String table;
    String output;
    String protoMsg;
  }
}
