# Zerobus SDK Examples

This directory contains example applications demonstrating different usage patterns of the Zerobus Ingest SDK for Java.

## Examples

The examples are organized by record type and schema approach:

### Protobuf Records (`proto/`)

#### Compiled Schema (`proto/compiled/`)

Use compiled protobuf classes generated from `.proto` files. Best for type-safe development with IDE autocompletion.

| Example | Description |
|---------|-------------|
| `SingleRecordExample.java` | Ingest protobuf records one at a time using generated classes |
| `BatchRecordExample.java` | Ingest protobuf records in batches using generated classes |

#### Dynamic Schema (`proto/dynamic/`)

Use runtime-loaded protobuf descriptors with `DynamicMessage`. Best for multi-tenant systems, schema registries, or generic data pipelines.

| Example | Description |
|---------|-------------|
| `SingleRecordExample.java` | Ingest records one at a time with runtime schema |
| `BatchRecordExample.java` | Ingest records in batches with runtime schema |

### JSON Records (`json/`)

| Example | Description |
|---------|-------------|
| `SingleRecordExample.java` | Ingest JSON records one at a time |
| `BatchRecordExample.java` | Ingest JSON records in batches |

## Configuration

Before running the examples, update the following constants in each example file:

```java
private static final String SERVER_ENDPOINT = "your-shard-id.zerobus.region.cloud.databricks.com";
private static final String UNITY_CATALOG_ENDPOINT = "https://your-workspace.cloud.databricks.com";
private static final String TABLE_NAME = "catalog.schema.table";
private static final String CLIENT_ID = "your-oauth-client-id";
private static final String CLIENT_SECRET = "your-oauth-client-secret";
```

## Protobuf Schema

### Compiled Schema Examples

The static schema examples use an `AirQuality` message defined in `record.proto`:

```proto
syntax = "proto2";

message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

To use your own schema:
1. Define your `.proto` file
2. Generate Java classes: `protoc --java_out=. your_schema.proto`
3. Update the examples to use your message type instead of `Record.AirQuality`

### Dynamic Schema Examples

The dynamic schema examples build protobuf descriptors programmatically at runtime using the Protocol Buffers `DescriptorProtos` API:

```java
// Build descriptor programmatically
DescriptorProtos.DescriptorProto descriptorProto = DescriptorProtos.DescriptorProto.newBuilder()
    .setName("AirQuality")
    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("device_name").setNumber(1)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("temp").setNumber(2)
        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
    .build();

// Create runtime descriptor
FileDescriptorProto fileDescriptorProto = FileDescriptorProto.newBuilder()
    .addMessageType(descriptorProto)
    .build();
FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileDescriptorProto, new FileDescriptor[]{});
Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName("AirQuality");
```

This approach is ideal for:
- Multi-tenant systems where schemas vary per tenant
- Schema registries that provide schema definitions at runtime
- Generic data pipelines that need runtime flexibility

## Running the Examples

The examples are part of a Maven module and can be run using Maven from the repository root.

### Build First

```bash
# From the repository root, build all modules
mvn clean compile
```

### Proto Compiled Schema Examples
```bash
# Single record
mvn exec:java -pl sdk/examples \
  -Dexec.mainClass=com.databricks.zerobus.examples.proto.compiled.SingleRecordExample

# Batch
mvn exec:java -pl sdk/examples \
  -Dexec.mainClass=com.databricks.zerobus.examples.proto.compiled.BatchRecordExample
```

### Proto Dynamic Schema Examples
```bash
# Single record
mvn exec:java -pl sdk/examples \
  -Dexec.mainClass=com.databricks.zerobus.examples.proto.dynamic.SingleRecordExample

# Batch
mvn exec:java -pl sdk/examples \
  -Dexec.mainClass=com.databricks.zerobus.examples.proto.dynamic.BatchRecordExample
```

### JSON Examples
```bash
# Single record
mvn exec:java -pl sdk/examples \
  -Dexec.mainClass=com.databricks.zerobus.examples.json.SingleRecordExample

# Batch
mvn exec:java -pl sdk/examples \
  -Dexec.mainClass=com.databricks.zerobus.examples.json.BatchRecordExample
```

## Advanced Configuration

### Custom Authentication

For custom authentication strategies, implement the `HeadersProvider` interface:

```java
HeadersProvider customProvider = new HeadersProvider() {
    @Override
    public Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("authorization", "Bearer " + getMyToken());
        headers.put("x-databricks-zerobus-table-name", TABLE_NAME);
        return headers;
    }
};

// Using the fluent builder API with custom headers provider
ProtoZerobusStream<Record.AirQuality> stream = sdk.streamBuilder(TABLE_NAME)
    .clientCredentials(CLIENT_ID, CLIENT_SECRET)
    .headersProvider(customProvider)  // Custom authentication
    .compiledProto(Record.AirQuality.getDefaultInstance())
    .build()
    .join();
```

### Custom TLS Configuration

For custom TLS settings, extend the `TlsConfig` class:

```java
TlsConfig customTls = new SecureTlsConfig(); // Uses system CA certificates (default)

// Using the fluent builder API with custom TLS config
ProtoZerobusStream<Record.AirQuality> stream = sdk.streamBuilder(TABLE_NAME)
    .clientCredentials(CLIENT_ID, CLIENT_SECRET)
    .tlsConfig(customTls)  // Custom TLS configuration
    .compiledProto(Record.AirQuality.getDefaultInstance())
    .build()
    .join();
```

### Custom Executor

For custom thread management, use the SDK builder:

```java
ExecutorService myExecutor = Executors.newFixedThreadPool(10);
ZerobusSdk sdk = ZerobusSdk.builder(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT)
    .executor(myExecutor)
    .build();
```

### Stream Configuration Options

All configuration options can be set on the stream builder:

```java
ProtoZerobusStream<Record.AirQuality> stream = sdk.streamBuilder(TABLE_NAME)
    .clientCredentials(CLIENT_ID, CLIENT_SECRET)
    .maxInflightRequests(50000)       // Max unacked requests
    .recovery(true)                    // Enable auto-recovery
    .recoveryRetries(3)               // Max recovery attempts
    .recoveryTimeoutMs(15000)         // Recovery timeout
    .recoveryBackoffMs(2000)          // Backoff between retries
    .flushTimeoutMs(300000)           // Flush timeout
    .serverLackOfAckTimeoutMs(60000)  // Server ack timeout
    .maxMessageSizeBytes(10485760)    // 10MB max message size
    .offsetCallback(offset ->
        System.out.println("Acknowledged: " + offset))
    .compiledProto(Record.AirQuality.getDefaultInstance())
    .build()
    .join();
```

## Best Practices

1. **Choose the right schema approach**:
   - Compiled schema: Type safety, IDE support, compile-time validation
   - Dynamic schema: Runtime flexibility, multi-tenant support, schema registry integration
2. **Choose the right record type**: Use protobuf for efficiency, JSON for flexibility
3. **Use batching for high throughput**: `ingestBatch()` reduces per-record overhead
4. **Handle errors**: Always wrap ingestion in try-catch blocks
5. **Close streams**: Always close streams in a `finally` block or use try-with-resources
6. **Tune buffer size**: Adjust `maxInflightRequests` based on your throughput needs
7. **SDK lifecycle**: The SDK implements `AutoCloseable` - use try-with-resources or call `close()` for explicit cleanup (optional, daemon threads clean up on JVM shutdown)

## Common Issues

### Authentication Failures
- Verify your CLIENT_ID and CLIENT_SECRET are correct
- Check that your OAuth client has permissions for the target table

### Slow Performance
- Use batch ingestion for better throughput
- Increase `maxInflightRequests` in stream configuration
- Check network connectivity to the Zerobus endpoint

## Additional Resources

- [SDK Documentation](../../README.md)
- [Protocol Buffers Guide](https://developers.google.com/protocol-buffers)
- [Databricks Documentation](https://docs.databricks.com)
