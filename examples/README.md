# Zerobus SDK Examples

This directory contains example applications demonstrating different usage patterns of the Zerobus Ingest SDK for Java.

## Examples

### 1. Blocking Ingestion (`BlockingIngestionExample.java`)

Demonstrates synchronous record ingestion where each record is waited for before proceeding to the next.

**Best for:**
- Low-volume ingestion (< 1000 records/sec)
- Use cases requiring immediate confirmation per record
- Critical data where you need to handle errors immediately

**Key features:**
- Waits for each record to be durably written
- Simple error handling
- Predictable behavior
- Lower throughput

**Run:**
```bash
javac -cp "../target/zerobus-ingest-sdk-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar" \
  src/main/java/com/databricks/zerobus/examples/BlockingIngestionExample.java

java -cp "../target/zerobus-ingest-sdk-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar:src/main/java" \
  com.databricks.zerobus.examples.BlockingIngestionExample
```

### 2. Non-Blocking Ingestion (`NonBlockingIngestionExample.java`)

Demonstrates asynchronous record ingestion for maximum throughput.

**Best for:**
- High-volume ingestion (> 10,000 records/sec)
- Batch processing scenarios
- Stream processing applications
- Maximum throughput requirements

**Key features:**
- Asynchronous ingestion with CompletableFutures
- Automatic buffering and flow control
- Ack callback for progress tracking
- Batch flush at the end
- Higher throughput

**Run:**
```bash
javac -cp "../target/zerobus-ingest-sdk-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar" \
  src/main/java/com/databricks/zerobus/examples/NonBlockingIngestionExample.java

java -cp "../target/zerobus-ingest-sdk-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar:src/main/java" \
  com.databricks.zerobus.examples.NonBlockingIngestionExample
```

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

The examples use an `AirQuality` message defined as:

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

## Performance Comparison

Typical performance characteristics (results may vary):

| Metric | Blocking | Non-Blocking |
|--------|----------|--------------|
| Throughput | ~100-500 records/sec | ~10,000-50,000 records/sec |
| Latency (avg) | Low per record | Higher per record, lower overall |
| Memory usage | Low | Medium (buffering) |
| Complexity | Simple | Moderate |
| Error handling | Immediate | Deferred to flush |

## Best Practices

1. **Choose the right pattern**: Use blocking for low-volume/critical data, non-blocking for high-volume
2. **Monitor progress**: Use `ackCallback` in non-blocking mode to track progress
3. **Handle errors**: Always wrap ingestion in try-catch blocks
4. **Close streams**: Always close streams in a `finally` block or use try-with-resources
5. **Tune buffer size**: Adjust `maxInflightRecords` based on your throughput needs

## Common Issues

### Out of Memory
Increase JVM heap size:
```bash
java -Xmx4g -cp ... com.databricks.zerobus.examples.NonBlockingIngestionExample
```

### Authentication Failures
- Verify your CLIENT_ID and CLIENT_SECRET are correct
- Check that your OAuth client has permissions for the target table

### Slow Performance
- Use non-blocking mode for better throughput
- Increase `maxInflightRecords` in stream configuration
- Check network connectivity to the Zerobus endpoint

## Additional Resources

- [SDK Documentation](../README.md)
- [Protocol Buffers Guide](https://developers.google.com/protocol-buffers)
- [Databricks Documentation](https://docs.databricks.com)
