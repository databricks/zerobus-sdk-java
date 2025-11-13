# Databricks Zerobus Ingest SDK for Java

[Public Preview](https://docs.databricks.com/release-notes/release-types.html): This SDK is supported for production use cases and is available to all customers. Databricks is actively working on stabilizing the Zerobus Ingest SDK for Java. Minor version updates may include backwards-incompatible changes.

We are keen to hear feedback from you on this SDK. Please [file issues](https://github.com/databricks/zerobus-sdk-java/issues), and we will address them.

The Databricks Zerobus Ingest SDK for Java provides a high-performance client for ingesting data directly into Databricks Delta tables using the Zerobus streaming protocol. | See also the [SDK for Rust](https://github.com/databricks/zerobus-sdk-rs) | See also the [SDK for Python](https://github.com/databricks/zerobus-sdk-py)

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Quick Start User Guide](#quick-start-user-guide)
  - [Prerequisites](#prerequisites)
  - [Building Your Application](#building-your-application)
  - [Define Your Protocol Buffer Schema](#define-your-protocol-buffer-schema)
  - [Generate Protocol Buffer Schema from Unity Catalog (Alternative)](#generate-protocol-buffer-schema-from-unity-catalog-alternative)
  - [Write Your Client Code](#write-your-client-code)
  - [Compile and Run](#compile-and-run)
- [Usage Examples](#usage-examples)
  - [Blocking Ingestion](#blocking-ingestion)
  - [Non-Blocking Ingestion](#non-blocking-ingestion)
- [Configuration](#configuration)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)

## Features

- **High-throughput ingestion**: Optimized for high-volume data ingestion
- **Automatic recovery**: Built-in retry and recovery mechanisms
- **Flexible configuration**: Customizable stream behavior and timeouts
- **Protocol Buffers**: Strongly-typed schema using protobuf
- **OAuth 2.0 authentication**: Secure authentication with client credentials

## Requirements

### Runtime Requirements

- **Java**: 8 or higher - [Download Java](https://adoptium.net/)
- **Databricks workspace** with Zerobus access enabled

### Dependencies

**When using the fat JAR** (recommended for most users):
- No additional dependencies required - all dependencies are bundled

**When using the regular JAR**:
- [`protobuf-java` 4.33.0](https://mvnrepository.com/artifact/com.google.protobuf/protobuf-java/4.33.0)
- [`grpc-netty-shaded` 1.76.0](https://mvnrepository.com/artifact/io.grpc/grpc-netty-shaded/1.76.0)
- [`grpc-protobuf` 1.76.0](https://mvnrepository.com/artifact/io.grpc/grpc-protobuf/1.76.0)
- [`grpc-stub` 1.76.0](https://mvnrepository.com/artifact/io.grpc/grpc-stub/1.76.0)
- [`javax.annotation-api` 1.3.2](https://mvnrepository.com/artifact/javax.annotation/javax.annotation-api/1.3.2)
- [`slf4j-api` 2.0.17](https://mvnrepository.com/artifact/org.slf4j/slf4j-api/2.0.17)
- An SLF4J implementation such as [`slf4j-simple` 2.0.17](https://mvnrepository.com/artifact/org.slf4j/slf4j-simple/2.0.17) or [`logback-classic` 1.4.14](https://mvnrepository.com/artifact/ch.qos.logback/logback-classic/1.4.14)

### Build Requirements (only for building from source)

- **Java**: 8 or higher - [Download Java](https://adoptium.net/)
- **Maven**: 3.6 or higher - [Download Maven](https://maven.apache.org/download.cgi)
- **Protocol Buffers Compiler** (`protoc`): 33.0 - [Download protoc](https://github.com/protocolbuffers/protobuf/releases/tag/v33.0) (for compiling your own `.proto` schemas)

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

#### Option 1: Using Maven Central (Recommended)

**Regular JAR (with dependency management):**

Add the SDK as a dependency in your `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>com.databricks</groupId>
        <artifactId>zerobus-ingest-sdk</artifactId>
        <version>0.1.0</version>
    </dependency>
</dependencies>
```

Or with Gradle (`build.gradle`):

```groovy
dependencies {
    implementation 'com.databricks:zerobus-ingest-sdk:0.1.0'
}
```

**Important**: You must also add the required dependencies manually, as they are not automatically included:

```xml
<!-- Add these dependencies in addition to the SDK -->
<dependencies>
    <!-- Zerobus SDK -->
    <dependency>
        <groupId>com.databricks</groupId>
        <artifactId>zerobus-ingest-sdk</artifactId>
        <version>0.1.0</version>
    </dependency>

    <!-- Required dependencies -->
    <dependency>
        <groupId>com.google.protobuf</groupId>
        <artifactId>protobuf-java</artifactId>
        <version>4.33.0</version>
    </dependency>
    <dependency>
        <groupId>io.grpc</groupId>
        <artifactId>grpc-netty-shaded</artifactId>
        <version>1.76.0</version>
    </dependency>
    <dependency>
        <groupId>io.grpc</groupId>
        <artifactId>grpc-protobuf</artifactId>
        <version>1.76.0</version>
    </dependency>
    <dependency>
        <groupId>io.grpc</groupId>
        <artifactId>grpc-stub</artifactId>
        <version>1.76.0</version>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>2.0.17</version>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-simple</artifactId>
        <version>2.0.17</version>
    </dependency>
    <dependency>
        <groupId>javax.annotation</groupId>
        <artifactId>javax.annotation-api</artifactId>
        <version>1.3.2</version>
    </dependency>
</dependencies>
```

**Fat JAR (with all dependencies bundled):**

If you prefer the self-contained fat JAR with all dependencies included:

```xml
<dependencies>
    <dependency>
        <groupId>com.databricks</groupId>
        <artifactId>zerobus-ingest-sdk</artifactId>
        <version>0.1.0</version>
        <classifier>jar-with-dependencies</classifier>
    </dependency>
</dependencies>
```

Or with Gradle:

```groovy
dependencies {
    implementation 'com.databricks:zerobus-ingest-sdk:0.1.0:jar-with-dependencies'
}
```

**Note:** The fat JAR is typically not needed for Maven/Gradle projects. Use the regular JAR (without classifier) unless you have a specific reason to bundle all dependencies.

#### Option 2: Build from Source

Clone and build the SDK:

```bash
git clone https://github.com/databricks/zerobus-sdk-java.git
cd zerobus-sdk-java
mvn clean package
```

This generates two JAR files in the `target/` directory:

- **Regular JAR**: `zerobus-ingest-sdk-0.1.0.jar` (155KB)
  - Contains only the SDK classes
  - Requires all dependencies on the classpath

- **Fat JAR**: `zerobus-ingest-sdk-0.1.0-jar-with-dependencies.jar` (18MB)
  - Contains SDK classes plus all dependencies bundled
  - Self-contained, easier to deploy

**Which JAR to use?**
- **Regular JAR**: When using Maven/Gradle (recommended)
- **Fat JAR**: For standalone scripts or CLI tools without a build system

### Create Your Application Project

#### Using Maven

Create a new Maven project:

```bash
mkdir my-zerobus-app
cd my-zerobus-app
```

Create `pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>my-zerobus-app</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <!-- Zerobus SDK -->
        <dependency>
            <groupId>com.databricks</groupId>
            <artifactId>zerobus-ingest-sdk</artifactId>
            <version>0.1.0</version>
        </dependency>

        <!-- Required dependencies (see above for full list) -->
        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
            <version>4.33.0</version>
        </dependency>
        <!-- Add other dependencies from the list above -->
    </dependencies>
</project>
```

Create project structure:

```bash
mkdir -p src/main/java/com/example
mkdir -p src/main/proto
```

#### Define Your Protocol Buffer Schema

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

**Compile the protobuf:**

```bash
protoc --java_out=src/main/java src/main/proto/record.proto
```

This generates `src/main/java/com/example/proto/Record.java`.

**Note**: Ensure you have `protoc` version 33.0 installed. [Download protoc](https://github.com/protocolbuffers/protobuf/releases/tag/v33.0) if needed. The generated Java files are compatible with `protobuf-java` 4.33.0.

### Generate Protocol Buffer Schema from Unity Catalog (Alternative)

Instead of manually writing and compiling your protobuf schema, you can automatically generate it from an existing Unity Catalog table schema using the included `GenerateProto` tool.

#### Using the Proto Generation Tool

The `GenerateProto` tool fetches your table schema from Unity Catalog and generates a corresponding proto2 definition file with the correct type mappings.

**First, download the fat JAR:**

The proto generation tool requires the fat JAR (all dependencies included):

```bash
# Download from Maven Central
wget https://repo1.maven.org/maven2/com/databricks/zerobus-ingest-sdk/0.1.0/zerobus-ingest-sdk-0.1.0-jar-with-dependencies.jar

# Or if you built from source, it's in target/
# cp target/zerobus-ingest-sdk-0.1.0-jar-with-dependencies.jar .
```

**Run the tool:**

```bash
java -jar zerobus-ingest-sdk-0.1.0-jar-with-dependencies.jar \
  --uc-endpoint "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com" \
  --client-id "your-service-principal-application-id" \
  --client-secret "your-service-principal-secret" \
  --table "main.default.air_quality" \
  --output "src/main/proto/record.proto" \
  --proto-msg "AirQuality"
```

**Parameters:**
- `--uc-endpoint`: Your workspace URL (e.g., `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com`)
- `--client-id`: Service principal application ID
- `--client-secret`: Service principal secret
- `--table`: Fully qualified table name (catalog.schema.table)
- `--output`: Output path for the generated proto file
- `--proto-msg`: (Optional) Name for the protobuf message (defaults to table name)

**Example:**

For a table defined as:
```sql
CREATE TABLE main.default.air_quality (
    device_name STRING,
    temp INT,
    humidity BIGINT
)
USING DELTA;
```

Running the generation tool will create `src/main/proto/record.proto`:
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

After generating the proto file, compile it as shown above:
```bash
protoc --java_out=src/main/java src/main/proto/record.proto
```

**Type Mappings:**

The tool automatically maps Unity Catalog types to proto2 types:

| Delta Type | Proto2 Type |
|-----------|-------------|
| INT, SMALLINT, SHORT | int32 |
| BIGINT, LONG | int64 |
| FLOAT | float |
| DOUBLE | double |
| STRING, VARCHAR | string |
| BOOLEAN | bool |
| BINARY | bytes |
| DATE | int32 |
| TIMESTAMP | int64 |
| ARRAY\<type\> | repeated type |
| MAP\<key, value\> | map\<key, value\> |
| STRUCT\<fields\> | nested message |

**Benefits:**
- No manual schema creation required
- Ensures schema consistency between your table and protobuf definitions
- Automatically handles complex types (arrays, maps, structs)
- Reduces errors from manual type mapping
- No need to clone the repository - runs directly from the SDK JAR

For detailed documentation and examples, see [tools/README.md](tools/README.md).

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
        ).join();

        try {
            // Ingest records
            for (int i = 0; i < 100; i++) {
                AirQuality record = AirQuality.newBuilder()
                    .setDeviceName("sensor-" + (i % 10))
                    .setTemp(20 + (i % 15))
                    .setHumidity(50 + (i % 40))
                    .build();

                stream.ingestRecord(record).join(); // Wait for durability

                System.out.println("Ingested record " + (i + 1));
            }

            System.out.println("Successfully ingested 100 records!");
        } finally {
            stream.close();
        }
    }
}
```

#### Compile and Run

**Using Maven:**

```bash
# First, compile the proto file to generate Java classes
protoc --java_out=src/main/java src/main/proto/record.proto

# Compile and run
mvn compile
mvn exec:java -Dexec.mainClass="com.example.ZerobusClient"
```

**Or build as standalone JAR:**

```bash
# Generate proto classes
protoc --java_out=src/main/java src/main/proto/record.proto

# Package into executable JAR (add maven-shade-plugin to pom.xml)
mvn package

# Run the JAR
java -jar target/my-zerobus-app-1.0-SNAPSHOT.jar
```

**Using downloaded JAR (without Maven):**

```bash
# Generate proto classes
protoc --java_out=src/main/java src/main/proto/record.proto

# Compile
javac -cp "lib/*" -d out src/main/java/com/example/ZerobusClient.java src/main/java/com/example/proto/Record.java

# Run
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
).join();

try {
    for (int i = 0; i < 1000; i++) {
        AirQuality record = AirQuality.newBuilder()
            .setDeviceName("sensor-" + i)
            .setTemp(20 + i % 15)
            .setHumidity(50 + i % 40)
            .build();

        stream.ingestRecord(record).join(); // Wait for durability
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
).join();

List<CompletableFuture<Void>> futures = new ArrayList<>();

try {
    for (int i = 0; i < 100000; i++) {
        AirQuality record = AirQuality.newBuilder()
            .setDeviceName("sensor-" + (i % 10))
            .setTemp(20 + i % 15)
            .setHumidity(50 + i % 40)
            .build();

        futures.add(stream.ingestRecord(record));
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

The Databricks Zerobus Ingest SDK for Java uses the standard [SLF4J logging framework](https://www.slf4j.org/). The SDK only depends on `slf4j-api`, which means **you need to add an SLF4J implementation** to your classpath to see log output.

### Adding a Logging Implementation

**Option 1: Using slf4j-simple** (simplest for getting started)

Add to your Maven dependencies:
```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>2.0.17</version>
</dependency>
```

Control log levels with system properties:
```bash
java -Dorg.slf4j.simpleLogger.log.com.databricks.zerobus=debug -cp "lib/*:out" com.example.ZerobusClient
```

Available log levels: `trace`, `debug`, `info`, `warn`, `error`

**Option 2: Using Logback** (recommended for production)

Add to your Maven dependencies:
```xml
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.4.14</version>
</dependency>
```

Create `logback.xml` in your resources directory:
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

**Option 3: Using Log4j 2**

Add to your Maven dependencies:
```xml
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-slf4j-impl</artifactId>
    <version>2.20.0</version>
</dependency>
```

Create `log4j2.xml` in your resources directory:
```xml
<Configuration>
    <Appenders>
        <Console name="Console" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
    </Appenders>
    <Loggers>
        <Logger name="com.databricks.zerobus" level="debug"/>
        <Root level="info">
            <AppenderRef ref="Console"/>
        </Root>
    </Loggers>
</Configuration>
```

### No Logging Implementation

If you don't add an SLF4J implementation, you'll see a warning like:
```
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
```

The SDK will still work, but no log messages will be output.

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
<RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
    TableProperties<RecordType> tableProperties,
    String clientId,
    String clientSecret,
    StreamConfigurationOptions options
)
```
Creates a new ingestion stream with custom configuration. Returns a CompletableFuture that completes when the stream is ready.

```java
<RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
    TableProperties<RecordType> tableProperties,
    String clientId,
    String clientSecret
)
```
Creates a new ingestion stream with default configuration. Returns a CompletableFuture that completes when the stream is ready.

```java
<RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> recreateStream(
    ZerobusStream<RecordType> stream
)
```
Recreates a failed stream, resending unacknowledged records. Returns a CompletableFuture that completes when the stream is ready.

---

### ZerobusStream\<RecordType\>

Represents an active ingestion stream.

**Methods:**

```java
CompletableFuture<Void> ingestRecord(RecordType record) throws ZerobusException
```
Ingests a single record into the stream. Returns a future that completes when the record is durably written to storage.

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
7. **Proto generation**: Use the built-in `GenerateProto` tool to automatically generate proto files from your table schemas
