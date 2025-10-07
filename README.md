# Databricks Zerobus Ingest SDK for Java

[![Maven Central](https://img.shields.io/badge/maven--central-0.1.0--SNAPSHOT-blue)](https://central.sonatype.com/)
[![Java](https://img.shields.io/badge/java-8%2B-blue)](https://www.oracle.com/java/)

The Databricks Zerobus Ingest SDK for Java provides a high-performance client for ingesting data directly into Databricks Delta tables using the Zerobus streaming protocol.

## Features

- **High-throughput ingestion**: Optimized for high-volume data ingestion
- **Automatic recovery**: Built-in retry and recovery mechanisms
- **Flexible configuration**: Customizable stream behavior and timeouts
- **Protocol Buffers**: Strongly-typed schema using protobuf
- **OAuth 2.0 authentication**: Secure authentication with client credentials

## Requirements

- Java 8 or higher
- Maven 3.6+ (for building from source)
- Databricks workspace with Zerobus access

## Installation

### Maven

```xml
<dependency>
    <groupId>com.databricks</groupId>
    <artifactId>zerobus-ingest-sdk-java</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Gradle

```gradle
implementation 'com.databricks:zerobus-ingest-sdk-java:0.1.0-SNAPSHOT'
```

## Quick Start

```java
import com.databricks.zerobus.*;

// Initialize the SDK
ZerobusSdk sdk = new ZerobusSdk(
    "your-shard-id.zerobus.region.cloud.databricks.com",
    "https://your-workspace.cloud.databricks.com"
);

// Define your protobuf message
TableProperties<YourMessage> tableProperties = new TableProperties<>(
    "catalog.schema.table",
    YourMessage.getDefaultInstance()
);

// Create a stream
ZerobusStream<YourMessage> stream = sdk.createStream(
    tableProperties,
    "your-client-id",
    "your-client-secret"
);

// Ingest records
YourMessage record = YourMessage.newBuilder()
    .setField1("value1")
    .setField2(123)
    .build();

IngestRecordResult result = stream.ingestRecord(record);
result.getWriteCompleted().join(); // Wait for durability

// Close the stream
stream.close();
```

## Usage Examples

See the `examples/` directory for complete working examples.

### Blocking Ingestion

Ingest records synchronously, waiting for each record to be acknowledged:

```java
ZerobusStream<AirQuality> stream = sdk.createStream(
    tableProperties,
    clientId,
    clientSecret
);

try {
    for (int i = 0; i < 1000; i++) {
        AirQuality record = AirQuality.newBuilder()
            .setDeviceName("sensor-" + i)
            .setTemp(20 + i % 15)
            .setHumidity(50 + i % 40)
            .build();

        IngestRecordResult result = stream.ingestRecord(record);
        result.getWriteCompleted().join(); // Wait for durability
    }
} finally {
    stream.close();
}
```

### Non-Blocking Ingestion

Ingest records asynchronously for maximum throughput:

```java
StreamConfigurationOptions options = StreamConfigurationOptions.builder()
    .setMaxInflightRecords(50000)
    .setAckCallback(response ->
        System.out.println("Acknowledged offset: " +
            response.getDurabilityAckUpToOffset()))
    .build();

ZerobusStream<AirQuality> stream = sdk.createStream(
    tableProperties,
    clientId,
    clientSecret,
    options
);

List<CompletableFuture<Void>> futures = new ArrayList<>();

try {
    for (int i = 0; i < 100000; i++) {
        AirQuality record = AirQuality.newBuilder()
            .setDeviceName("sensor-" + (i % 10))
            .setTemp(20 + i % 15)
            .setHumidity(50 + i % 40)
            .build();

        IngestRecordResult result = stream.ingestRecord(record);
        futures.add(result.getWriteCompleted());
    }

    // Flush and wait for all records
    stream.flush();
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
} finally {
    stream.close();
}
```

## Configuration

### Stream Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `maxInflightRecords` | 50000 | Maximum number of unacknowledged records |
| `recovery` | true | Enable automatic stream recovery |
| `recoveryTimeoutMs` | 15000 | Timeout for recovery operations (ms) |
| `recoveryBackoffMs` | 2000 | Delay between recovery attempts (ms) |
| `recoveryRetries` | 3 | Maximum number of recovery attempts |
| `flushTimeoutMs` | 300000 | Timeout for flush operations (ms) |
| `serverLackOfAckTimeoutMs` | 60000 | Server acknowledgment timeout (ms) |
| `ackCallback` | None | Callback invoked on record acknowledgment |

## Building from Source

```bash
git clone https://github.com/databricks/zerobus-sdk-java.git
cd zerobus-sdk-java
mvn clean package
```

This will generate:
- Regular JAR: `target/zerobus-ingest-sdk-java-0.1.0-SNAPSHOT.jar` (144KB)
- Fat JAR: `target/zerobus-ingest-sdk-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar` (18MB)

## Protocol Buffers

Define your data schema using Protocol Buffers:

```proto
syntax = "proto2";

message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

Generate Java classes:

```bash
protoc --java_out=src/main/java record.proto
```

## Error Handling

The SDK throws two types of exceptions:

- `ZerobusException`: Retriable errors (e.g., network issues)
- `NonRetriableException`: Non-retriable errors (e.g., invalid credentials)

```java
try {
    stream.ingestRecord(record);
} catch (NonRetriableException e) {
    // Fatal error - do not retry
    logger.error("Non-retriable error: " + e.getMessage());
    throw e;
} catch (ZerobusException e) {
    // Retriable error - can retry with backoff
    logger.warn("Retriable error: " + e.getMessage());
    // Implement retry logic
}
```

## Best Practices

1. **Reuse SDK instances**: Create one `ZerobusSdk` instance per application
2. **Stream lifecycle**: Always close streams in a `finally` block
3. **Batch size**: Adjust `maxInflightRecords` based on your throughput requirements
4. **Error handling**: Implement proper retry logic for retriable errors
5. **Monitoring**: Use `ackCallback` to track ingestion progress

## API Reference

### ZerobusSdk

Main entry point for the SDK.

**Constructor:**
```java
ZerobusSdk(String serverEndpoint, String unityCatalogEndpoint)
```

**Methods:**
- `createStream(TableProperties, String clientId, String clientSecret, StreamConfigurationOptions)` - Create a new ingestion stream
- `createStream(TableProperties, String clientId, String clientSecret)` - Create stream with default options
- `recreateStream(ZerobusStream)` - Recreate a failed stream

### ZerobusStream

Represents an active ingestion stream.

**Methods:**
- `ingestRecord(RecordType)` - Ingest a single record
- `flush()` - Flush all pending records
- `close()` - Close the stream gracefully
- `getState()` - Get current stream state
- `getStreamId()` - Get the stream ID

## Compatibility

- **Java Version**: Java 8 or higher
- **Databricks Runtime**: Compatible with all DBR versions supporting Zerobus
- **Protocol**: gRPC with Protocol Buffers

## License

Apache License 2.0
