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

### Runtime Requirements

- **Java**: 8 or higher
- **Databricks workspace** with Zerobus access enabled

### Dependencies

**When using the fat JAR** (recommended for most users):
- No additional dependencies required - all dependencies are bundled
- Includes `slf4j-simple` for logging out of the box

**When using the regular JAR**:
- `protobuf-java` 3.24.0
- `grpc-netty-shaded` 1.58.0
- `grpc-protobuf` 1.58.0
- `grpc-stub` 1.58.0
- `javax.annotation-api` 1.3.2
- `slf4j-api` 1.7.36
- `slf4j-simple` 1.7.36 (or substitute your own SLF4J implementation like `logback-classic` 1.2.11)

### Build Requirements (only for building from source)

- **Maven**: 3.6 or higher
- **Protocol Buffers Compiler** (`protoc`): 3.24.0 (for compiling your own `.proto` schemas)

## Quick Start User Guide

### Prerequisites

Before using the SDK, you'll need the following:

#### 1. Workspace URL and Workspace ID

After logging into your Databricks workspace, look at the browser URL:

```
https://<databricks-instance>.cloud.databricks.com/o=<workspace-id>
```

- **Workspace URL**: The part before `/o=` → `https://<databricks-instance>.cloud.databricks.com`
- **Workspace ID**: The part after `/o=` → `<workspace-id>`

Example:
- Full URL: `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com/o=1234567890123456`
- Workspace URL: `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com`
- Workspace ID: `1234567890123456`

#### 2. Create a Delta Table

Create a table using Databricks SQL:

```sql
CREATE TABLE <catalog_name>.default.air_quality (
    device_name STRING,
    temp INT,
    humidity BIGINT
)
USING DELTA;
```

Replace `<catalog_name>` with your catalog name (e.g., `main`).

#### 3. Create a Service Principal

1. Navigate to **Settings > Identity and Access** in your Databricks workspace
2. Click **Service principals** and create a new service principal
3. Generate a new secret for the service principal and save it securely
4. Grant the following permissions:
   - `USE_CATALOG` on the catalog (e.g., `main`)
   - `USE_SCHEMA` on the schema (e.g., `default`)
   - `MODIFY` and `SELECT` on the table (e.g., `air_quality`)

Grant permissions using SQL:

```sql
-- Grant catalog permission
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<service-principal-application-id>`;

-- Grant schema permission
GRANT USE SCHEMA ON SCHEMA <catalog_name>.default TO `<service-principal-application-id>`;

-- Grant table permissions
GRANT SELECT, MODIFY ON TABLE <catalog_name>.default.air_quality TO `<service-principal-application-id>`;
```

### Building Your Application

#### 1. Build the SDK from Source

Clone and build the SDK:

```bash
git clone https://github.com/databricks/zerobus-sdk-java.git
cd zerobus-sdk-java
mvn clean package
```

This generates two JAR files in the `target/` directory:

- **Regular JAR**: `zerobus-ingest-sdk-java-0.1.0-SNAPSHOT.jar` (144KB)
  - Contains only the SDK classes
  - Requires all dependencies on the classpath

- **Fat JAR**: `zerobus-ingest-sdk-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar` (18MB)
  - Contains SDK classes plus all dependencies bundled
  - Self-contained, easier to deploy

**Which one to use?**
- Use the **fat JAR** for simple deployments and standalone applications
- Use the **regular JAR** when dependencies are managed by your build system (Maven/Gradle)

#### 2. Create Your Application Project

Create a new Java project:

```bash
mkdir my-zerobus-app
cd my-zerobus-app
mkdir -p src/main/java/com/example
mkdir -p src/main/proto
```

Project structure:
```
my-zerobus-app/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── example/
│       │           └── ZerobusClient.java
│       └── proto/
│           └── record.proto
└── lib/
    └── zerobus-ingest-sdk-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

Copy the fat JAR to your project:

```bash
mkdir lib
cp ../zerobus-sdk-java/target/zerobus-ingest-sdk-java-0.1.0-SNAPSHOT-jar-with-dependencies.jar lib/
```

#### 3. Define Your Protocol Buffer Schema

Create `src/main/proto/record.proto`:

```protobuf
syntax = "proto2";

package com.example;

option java_package = "com.example.proto";
option java_outer_classname = "Record";

message AirQuality {
    optional string device_name = 1;
    optional int32 temp = 2;
    optional int64 humidity = 3;
}
```

Compile the protobuf:

```bash
protoc --java_out=src/main/java src/main/proto/record.proto
```

This generates `src/main/java/com/example/proto/Record.java`.

#### 4. Write Your Client Code

Create `src/main/java/com/example/ZerobusClient.java`:

```java
package com.example;

import com.databricks.zerobus.*;
import com.example.proto.Record.AirQuality;

public class ZerobusClient {
    public static void main(String[] args) throws Exception {
        // Configuration
        String serverEndpoint = "1234567890123456.zerobus.us-west-2.cloud.databricks.com";
        String workspaceUrl = "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com";
        String tableName = "main.default.air_quality";
        String clientId = "your-service-principal-application-id";
        String clientSecret = "your-service-principal-secret";

        // Initialize SDK
        ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

        // Configure table properties
        TableProperties<AirQuality> tableProperties = new TableProperties<>(
            tableName,
            AirQuality.getDefaultInstance()
        );

        // Create stream
        ZerobusStream<AirQuality> stream = sdk.createStream(
            tableProperties,
            clientId,
            clientSecret
        );

        try {
            // Ingest records
            for (int i = 0; i < 100; i++) {
                AirQuality record = AirQuality.newBuilder()
                    .setDeviceName("sensor-" + (i % 10))
                    .setTemp(20 + (i % 15))
                    .setHumidity(50 + (i % 40))
                    .build();

                IngestRecordResult result = stream.ingestRecord(record);
                result.getWriteCompleted().join(); // Wait for durability

                System.out.println("Ingested record " + (i + 1));
            }

            System.out.println("Successfully ingested 100 records!");
        } finally {
            stream.close();
        }
    }
}
```

#### 5. Compile and Run

Compile your application:

```bash
javac -cp "lib/*" -d out src/main/java/com/example/ZerobusClient.java src/main/java/com/example/proto/Record.java
```

Run your application:

```bash
java -cp "lib/*:out" com.example.ZerobusClient
```

You should see output like:
```
Ingested record 1
Ingested record 2
...
Successfully ingested 100 records!
```

## Usage Examples

See the `examples/` directory for complete working examples:

- **BlockingIngestionExample.java** - Synchronous ingestion with progress tracking
- **NonBlockingIngestionExample.java** - High-throughput asynchronous ingestion

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

## Logging

The Databricks Zerobus Ingest SDK for Java uses the standard [SLF4J logging framework](https://www.slf4j.org/) and includes `slf4j-simple` by default, so **logging works out of the box** with no additional configuration required.

### Controlling Log Levels

By default, the SDK logs at **INFO** level. To enable debug logging, set the system property when running your application:

```bash
java -Dorg.slf4j.simpleLogger.log.com.databricks.zerobus=debug -cp "lib/*:out" com.example.ZerobusClient
```

Available log levels: `trace`, `debug`, `info`, `warn`, `error`

### Using a Different Logging Framework

If you prefer a different logging framework like **Logback** or **Log4j**, you can substitute `slf4j-simple`:

**With Logback**, add the following to your `logback.xml`:

```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <logger name="com.databricks.zerobus" level="DEBUG"/>

    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

**With Log4j**, add to your `log4j.properties`:

```properties
log4j.logger.com.databricks.zerobus=DEBUG
```

To use a different logging framework, exclude `slf4j-simple` and add your preferred implementation to the classpath.

### What Gets Logged

At the **DEBUG** level, the SDK logs:
- Stream lifecycle events (creation, recovery, closure)
- Record ingestion progress
- Server acknowledgments
- Token generation events
- gRPC connection details

At the **INFO** level, the SDK logs:
- Important stream state transitions
- Successful stream operations

At the **WARN** level, the SDK logs:
- Stream recovery attempts
- Retryable errors

At the **ERROR** level, the SDK logs:
- Non-retriable errors
- Stream failures

## Error Handling

The SDK throws two types of exceptions:

- `ZerobusException`: Retriable errors (e.g., network issues, temporary server errors)
- `NonRetriableException`: Non-retriable errors (e.g., invalid credentials, missing table)

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

## API Reference

### ZerobusSdk

Main entry point for the SDK.

**Constructor:**
```java
ZerobusSdk(String serverEndpoint, String unityCatalogEndpoint)
```
- `serverEndpoint` - The Zerobus gRPC endpoint (e.g., `<workspace-id>.zerobus.region.cloud.databricks.com`)
- `unityCatalogEndpoint` - The Unity Catalog endpoint (your workspace URL)

**Methods:**

```java
<RecordType extends Message> ZerobusStream<RecordType> createStream(
    TableProperties<RecordType> tableProperties,
    String clientId,
    String clientSecret,
    StreamConfigurationOptions options
) throws ZerobusException
```
Creates a new ingestion stream with custom configuration.

```java
<RecordType extends Message> ZerobusStream<RecordType> createStream(
    TableProperties<RecordType> tableProperties,
    String clientId,
    String clientSecret
) throws ZerobusException
```
Creates a new ingestion stream with default configuration.

```java
<RecordType extends Message> ZerobusStream<RecordType> recreateStream(
    ZerobusStream<RecordType> stream
) throws ZerobusException
```
Recreates a failed stream, resending unacknowledged records.

---

### ZerobusStream\<RecordType\>

Represents an active ingestion stream.

**Methods:**

```java
IngestRecordResult ingestRecord(RecordType record)
```
Ingests a single record into the stream. Returns futures for tracking ingestion progress.

```java
void flush() throws ZerobusException
```
Flushes all pending records and waits for server acknowledgment. Does not close the stream.

```java
void close() throws ZerobusException
```
Flushes and closes the stream gracefully. Always call in a `finally` block.

```java
StreamState getState()
```
Returns the current stream state (`UNINITIALIZED`, `OPENED`, `FLUSHING`, `RECOVERING`, `CLOSED`, `FAILED`).

```java
String getStreamId()
```
Returns the unique stream ID assigned by the server.

```java
TableProperties<RecordType> getTableProperties()
```
Returns the table properties for this stream.

```java
StreamConfigurationOptions getOptions()
```
Returns the stream configuration options.

---

### TableProperties\<RecordType\>

Configuration for the target table.

**Constructor:**
```java
TableProperties(String tableName, RecordType defaultInstance)
```
- `tableName` - Fully qualified table name (e.g., `catalog.schema.table`)
- `defaultInstance` - Protobuf message default instance (e.g., `MyMessage.getDefaultInstance()`)

**Methods:**

```java
String getTableName()
```
Returns the table name.

```java
Message getDefaultInstance()
```
Returns the protobuf message default instance.

---

### StreamConfigurationOptions

Configuration options for stream behavior.

**Static Methods:**

```java
static StreamConfigurationOptions getDefault()
```
Returns default configuration options.

```java
static StreamConfigurationOptionsBuilder builder()
```
Returns a new builder for creating custom configurations.

---

### StreamConfigurationOptions.StreamConfigurationOptionsBuilder

Builder for creating `StreamConfigurationOptions`.

**Methods:**

```java
StreamConfigurationOptionsBuilder setMaxInflightRecords(int maxInflightRecords)
```
Sets the maximum number of unacknowledged records (default: 50000).

```java
StreamConfigurationOptionsBuilder setRecovery(boolean recovery)
```
Enables or disables automatic stream recovery (default: true).

```java
StreamConfigurationOptionsBuilder setRecoveryTimeoutMs(int recoveryTimeoutMs)
```
Sets the recovery operation timeout in milliseconds (default: 15000).

```java
StreamConfigurationOptionsBuilder setRecoveryBackoffMs(int recoveryBackoffMs)
```
Sets the delay between recovery attempts in milliseconds (default: 2000).

```java
StreamConfigurationOptionsBuilder setRecoveryRetries(int recoveryRetries)
```
Sets the maximum number of recovery attempts (default: 3).

```java
StreamConfigurationOptionsBuilder setFlushTimeoutMs(int flushTimeoutMs)
```
Sets the flush operation timeout in milliseconds (default: 300000).

```java
StreamConfigurationOptionsBuilder setServerLackOfAckTimeoutMs(int serverLackOfAckTimeoutMs)
```
Sets the server acknowledgment timeout in milliseconds (default: 60000).

```java
StreamConfigurationOptionsBuilder setAckCallback(Consumer<IngestRecordResponse> ackCallback)
```
Sets a callback to be invoked when records are acknowledged by the server.

```java
StreamConfigurationOptions build()
```
Builds and returns the `StreamConfigurationOptions` instance.

---

### IngestRecordResult

Result of an asynchronous record ingestion operation.

**Methods:**

```java
CompletableFuture<Void> getRecordAccepted()
```
Returns a future that completes when the SDK accepts the record for processing (fast).

```java
CompletableFuture<Void> getWriteCompleted()
```
Returns a future that completes when the record is durably written to storage (slower).

---

### IngestRecordResponse

Server acknowledgment response containing durability information.

**Methods:**

```java
long getDurabilityAckUpToOffset()
```
Returns the offset up to which all records have been durably written.

---

### StreamState (Enum)

Represents the lifecycle state of a stream.

**Values:**
- `UNINITIALIZED` - Stream created but not yet initialized
- `OPENED` - Stream is open and accepting records
- `FLUSHING` - Stream is flushing pending records
- `RECOVERING` - Stream is recovering from a failure
- `CLOSED` - Stream has been gracefully closed
- `FAILED` - Stream has failed and cannot be recovered

---

### ZerobusException

Base exception for retriable errors.

**Constructors:**
```java
ZerobusException(String message)
ZerobusException(String message, Throwable cause)
```

---

### NonRetriableException

Exception for non-retriable errors (extends `ZerobusException`).

**Constructors:**
```java
NonRetriableException(String message)
NonRetriableException(String message, Throwable cause)
```

## Best Practices

1. **Reuse SDK instances**: Create one `ZerobusSdk` instance per application
2. **Stream lifecycle**: Always close streams in a `finally` block
3. **Batch size**: Adjust `maxInflightRecords` based on your throughput requirements
4. **Error handling**: Implement proper retry logic for retriable errors
5. **Monitoring**: Use `ackCallback` to track ingestion progress
6. **Token refresh**: Tokens are automatically refreshed on stream creation and recovery

## Disclaimer

Databricks is actively working on stabilizing the Zerobus Ingest SDK for Java. Minor version updates may include backwards-incompatible changes.
