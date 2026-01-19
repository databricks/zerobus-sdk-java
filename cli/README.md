# Zerobus CLI

Command-line tools for the Databricks Zerobus Ingest SDK.

## Installation

**Download from Maven Central:**

```bash
wget https://repo1.maven.org/maven2/com/databricks/zerobus-cli/0.1.0/zerobus-cli-0.1.0.jar
```

**Or build from source:**

```bash
git clone https://github.com/databricks/zerobus-sdk-java.git
cd zerobus-sdk-java
mvn clean package -pl cli -am
```

## Commands

### generate-proto

Generates a Protocol Buffer schema file from an existing Unity Catalog table schema. This eliminates the need to manually write proto files and ensures your schema matches the table definition.

**Usage:**

```bash
java -jar zerobus-cli-0.1.0.jar generate-proto \
  --uc-endpoint <endpoint> \
  --client-id <client-id> \
  --client-secret <client-secret> \
  --table <catalog.schema.table> \
  --output <output.proto> \
  [--proto-msg <message_name>]
```

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--uc-endpoint` | Yes | Your Databricks workspace URL (e.g., `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com`) |
| `--client-id` | Yes | Service principal application ID |
| `--client-secret` | Yes | Service principal secret |
| `--table` | Yes | Fully qualified table name (`catalog.schema.table`) |
| `--output` | Yes | Output path for the generated proto file |
| `--proto-msg` | No | Name for the protobuf message (defaults to table name) |

**Example:**

```bash
java -jar zerobus-cli-0.1.0.jar generate-proto \
  --uc-endpoint "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com" \
  --client-id "your-service-principal-application-id" \
  --client-secret "your-service-principal-secret" \
  --table "main.default.air_quality" \
  --output "src/main/proto/record.proto" \
  --proto-msg "AirQuality"
```

For a table defined as:

```sql
CREATE TABLE main.default.air_quality (
    device_name STRING,
    temp INT,
    humidity BIGINT
)
USING DELTA;
```

The tool generates:

```protobuf
syntax = "proto3";

message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

After generating, compile the proto file:

```bash
protoc --java_out=src/main/java src/main/proto/record.proto
```

## Type Mappings

The tool automatically maps Unity Catalog types to Protocol Buffer types:

| Unity Catalog Type | Proto Type |
|-------------------|------------|
| TINYINT, BYTE | int32 |
| SMALLINT, SHORT | int32 |
| INT | int32 |
| BIGINT, LONG | int64 |
| FLOAT | float |
| DOUBLE | double |
| STRING, VARCHAR | string |
| BOOLEAN | bool |
| BINARY | bytes |
| DATE | int32 |
| TIMESTAMP | int64 |
| TIMESTAMP_NTZ | int64 |
| VARIANT | string (JSON) |
| ARRAY\<type\> | repeated type |
| MAP\<key, value\> | map\<key, value\> |
| STRUCT\<fields\> | nested message |

### Complex Type Examples

**ARRAY:**

```sql
-- Table definition
tags ARRAY<STRING>
```

```protobuf
// Generated proto
repeated string tags = 1;
```

**MAP:**

```sql
-- Table definition
metadata MAP<STRING, INT>
```

```protobuf
// Generated proto
map<string, int32> metadata = 1;
```

**STRUCT:**

```sql
-- Table definition
address STRUCT<city: STRING, zip: INT>
```

```protobuf
// Generated proto
message Address {
    optional string city = 1;
    optional int32 zip = 2;
}
optional Address address = 1;
```

**Nested STRUCT:**

```sql
-- Table definition
person STRUCT<name: STRING, address: STRUCT<city: STRING, country: STRING>>
```

```protobuf
// Generated proto
message Person {
    message Address {
        optional string city = 1;
        optional string country = 2;
    }

    optional string name = 1;
    optional Address address = 2;
}
optional Person person = 1;
```

## Authentication

The CLI uses OAuth 2.0 client credentials flow to authenticate with Unity Catalog. You need a service principal with the following permissions on the target table:

- `USE_CATALOG` on the catalog
- `USE_SCHEMA` on the schema
- `SELECT` on the table (to read schema)

Grant permissions using SQL:

```sql
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<service-principal-application-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog_name>.<schema_name> TO `<service-principal-application-id>`;
GRANT SELECT ON TABLE <catalog_name>.<schema_name>.<table_name> TO `<service-principal-application-id>`;
```

## Error Handling

The CLI provides clear error messages for common issues:

| Error | Cause | Solution |
|-------|-------|----------|
| `OAuth request failed with status 401` | Invalid credentials | Verify client ID and secret |
| `OAuth request failed with status 403` | Insufficient permissions | Grant required permissions to service principal |
| `Failed to fetch table info with status 404` | Table not found | Check table name (catalog.schema.table) |
| `Unsupported column type` | Unsupported Delta type | Check type mappings above |

## Limitations

- Nested arrays (`ARRAY<ARRAY<...>>`) are not supported
- Arrays of maps (`ARRAY<MAP<...>>`) are not supported
- Maps with map values (`MAP<..., MAP<...>>`) are not supported
- Maps with array values (`MAP<..., ARRAY<...>>`) are not supported
- Maximum struct nesting depth is 100 levels
