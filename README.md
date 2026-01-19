# Databricks Zerobus Ingest SDK for Java

[![Build](https://github.com/databricks/zerobus-sdk-java/actions/workflows/push.yml/badge.svg)](https://github.com/databricks/zerobus-sdk-java/actions/workflows/push.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.databricks/zerobus-ingest-sdk)](https://search.maven.org/artifact/com.databricks/zerobus-ingest-sdk)
[![Javadoc](https://javadoc.io/badge2/com.databricks/zerobus-ingest-sdk/javadoc.svg)](https://javadoc.io/doc/com.databricks/zerobus-ingest-sdk)
![Coverage](https://raw.githubusercontent.com/databricks/zerobus-sdk-java/badges/.github/badges/jacoco.svg)
![Java](https://img.shields.io/badge/Java-8%2B-blue)
[![License](https://img.shields.io/badge/License-Databricks-blue.svg)](LICENSE)

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
  - [JSON Record Ingestion](#json-record-ingestion)
  - [Using Custom Headers Provider](#using-custom-headers-provider)
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
- **JSON record support**: Direct JSON ingestion without protobuf encoding
- **OAuth 2.0 authentication**: Secure authentication with client credentials
- **Custom headers provider**: Flexible authentication strategies and custom headers support

## Requirements

### Runtime Requirements

- **Java**: 8 or higher - [Download Java](https://adoptium.net/)
- **Databricks workspace** with Zerobus access enabled

### Dependencies

**When using the shaded JAR** (recommended for most users):
- No additional dependencies required - all dependencies are bundled and relocated

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
        <version>0.2.0</version>
    </dependency>
</dependencies>
```

Or with Gradle (`build.gradle`):

```groovy
dependencies {
    implementation 'com.databricks:zerobus-ingest-sdk:0.2.0'
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
        <version>0.2.0</version>
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

**Shaded JAR (with all dependencies bundled and relocated):**

If you prefer the self-contained shaded JAR with all dependencies included:

```xml
<dependencies>
    <dependency>
        <groupId>com.databricks</groupId>
        <artifactId>zerobus-ingest-sdk</artifactId>
        <version>0.2.0</version>
        <classifier>shaded</classifier>
    </dependency>
</dependencies>
```

Or with Gradle:

```groovy
dependencies {
    implementation 'com.databricks:zerobus-ingest-sdk:0.2.0:shaded'
}
```

**Note:** The shaded JAR is typically not needed for Maven/Gradle projects. Use the regular JAR (without classifier) unless you have a specific reason to bundle all dependencies.

#### Option 2: Build from Source

Clone and build:

```bash
git clone https://github.com/databricks/zerobus-sdk-java.git
cd zerobus-sdk-java
mvn clean package
```

This is a multi-module project:
- `common/` - Shared utilities (HTTP client and JSON parsing)
- `sdk/` - The Zerobus Ingest SDK library
- `cli/` - Command-line tools

**SDK JARs** (in `sdk/target/`):
- `zerobus-ingest-sdk-0.2.0.jar` (~240KB) - Library for your applications
- `zerobus-ingest-sdk-0.2.0-shaded.jar` (~20MB) - Library with all dependencies bundled

**CLI JAR** (in `cli/target/`):
- `zerobus-cli-0.1.0.jar` (~20MB) - Standalone command-line tool

**Which JAR to use?**
- **SDK Regular JAR**: When using Maven/Gradle (recommended for applications)
- **SDK Shaded JAR**: For standalone scripts without a build system
- **CLI JAR**: For generating proto schemas from Unity Catalog tables

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
            <version>0.2.0</version>
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

Instead of manually writing your protobuf schema, you can generate it from an existing Unity Catalog table using the Zerobus CLI:

```bash
java -jar zerobus-cli-0.1.0.jar generate-proto \
  --uc-endpoint "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com" \
  --client-id "your-service-principal-application-id" \
  --client-secret "your-service-principal-secret" \
  --table "main.default.air_quality" \
  --output "src/main/proto/record.proto"
```

After generating, compile the proto file:

```bash
protoc --java_out=src/main/java src/main/proto/record.proto
```

See the [CLI README](cli/README.md) for installation instructions, all parameters, type mappings, and complex type examples.

### Write Your Client Code

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

        // Initialize SDK (implements AutoCloseable)
        try (ZerobusSdk sdk = new ZerobusSdk(serverEndpoint, workspaceUrl)) {

            // Create stream using the fluent builder API
            // The builder returns a type-safe ProtoZerobusStream<AirQuality>
            ProtoZerobusStream<AirQuality> stream = sdk.streamBuilder(tableName)
                .clientCredentials(clientId, clientSecret)
                .compiledProto(AirQuality.getDefaultInstance())
                .build()
                .join();

            // Both SDK and stream implement AutoCloseable
            try {
                // Ingest records - type-safe: only AirQuality records accepted
                for (int i = 0; i < 100; i++) {
                    AirQuality record = AirQuality.newBuilder()
                        .setDeviceName("sensor-" + (i % 10))
                        .setTemp(20 + (i % 15))
                        .setHumidity(50 + (i % 40))
                        .build();

                    stream.ingest(record);

                    System.out.println("Ingested record " + (i + 1));
                }

                System.out.println("Successfully ingested 100 records!");
            } finally {
                stream.close();
            }
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

See the `sdk/examples/` directory for complete working examples organized by schema type:

**Proto Examples:**
- `proto/compiled/SingleRecordExample.java` - Single record ingestion with compiled proto
- `proto/compiled/BatchRecordExample.java` - Batch ingestion with compiled proto
- `proto/dynamic/SingleRecordExample.java` - Single record ingestion with dynamic proto
- `proto/dynamic/BatchRecordExample.java` - Batch ingestion with dynamic proto

**JSON Examples:**
- `json/SingleRecordExample.java` - Single JSON record ingestion
- `json/BatchRecordExample.java` - Batch JSON record ingestion

### Fluent Stream Builder API

The SDK provides a fluent builder API for creating streams with **compile-time type safety**. The step builder pattern enforces that you choose an authentication path before schema selection - the compiler prevents you from calling schema methods directly on `StreamBuilder`.

**Builder Flow:**

```
sdk.streamBuilder(tableName)          → StreamBuilder
    .clientCredentials(...)           → AuthenticatedStreamBuilder  (OAuth path)
    .unauthenticated()                → UnauthenticatedStreamBuilder (custom auth path)

    .maxInflightRequests(...)         → same builder (optional config methods)
    .headersProvider(...)             → same builder (optional, for custom headers)
    .compiledProto(...)               → ProtoStreamBuilder<T>       (or .dynamicProto() or .json())
    .build()                          → CompletableFuture<ProtoZerobusStream<T>>
```

**Example usage:**

```java
// OAuth authentication (most common)
ProtoZerobusStream<AirQuality> protoStream = sdk.streamBuilder(tableName)
    .clientCredentials(clientId, clientSecret)  // Returns AuthenticatedStreamBuilder
    .maxInflightRequests(50000)                 // Config methods
    .recovery(true)
    .compiledProto(AirQuality.getDefaultInstance())
    .build()
    .join();

// JSON stream with OAuth
JsonZerobusStream jsonStream = sdk.streamBuilder(tableName)
    .clientCredentials(clientId, clientSecret)
    .json()
    .build()
    .join();

// Custom authentication with headers provider
JsonZerobusStream stream = sdk.streamBuilder(tableName)
    .unauthenticated()                // Returns UnauthenticatedStreamBuilder
    .headersProvider(customProvider)  // Set custom auth headers
    .json()
    .build()
    .join();

// This won't compile - schema selection requires auth path first:
// sdk.streamBuilder(tableName)
//     .compiledProto(...)  // ❌ Compile error: method not found on StreamBuilder
```

**The step builder pattern ensures:**
- **Compile-time safety**: `clientCredentials()` and `unauthenticated()` return different builder types. Schema methods (`compiledProto`, `dynamicProto`, `json`) only exist on these builders, so the compiler enforces the correct order.
- **Type safety**: Schema selection determines the return type (`ProtoZerobusStream<T>` or `JsonZerobusStream`)
- **Validation**: Invalid parameters are caught at configuration time with clear error messages
- **Fluent API**: Configuration methods can be chained in any order after choosing auth path

### Compiled Proto Stream

For compiled protobuf schemas (generated from `.proto` files):

```java
ProtoZerobusStream<AirQuality> stream = sdk.streamBuilder(tableName)
    .clientCredentials(clientId, clientSecret)
    .compiledProto(AirQuality.getDefaultInstance())
    .build()
    .join();

try {
    for (int i = 0; i < 1000; i++) {
        AirQuality record = AirQuality.newBuilder()
            .setDeviceName("sensor-" + i)
            .setTemp(20 + i % 15)
            .setHumidity(50 + i % 40)
            .build();

        stream.ingest(record);  // Type-safe: only AirQuality accepted
    }
    stream.flush();
} finally {
    stream.close();
}
```

### Dynamic Proto Stream

For runtime-defined protobuf schemas:

```java
// Build descriptor programmatically
DescriptorProto descriptorProto = DescriptorProto.newBuilder()
    .setName("AirQuality")
    .addField(FieldDescriptorProto.newBuilder()
        .setName("device_name").setNumber(1)
        .setType(FieldDescriptorProto.Type.TYPE_STRING)
        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
    .addField(FieldDescriptorProto.newBuilder()
        .setName("temp").setNumber(2)
        .setType(FieldDescriptorProto.Type.TYPE_INT32)
        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
    .build();

// Create descriptor from proto
FileDescriptorProto fileDescriptorProto = FileDescriptorProto.newBuilder()
    .addMessageType(descriptorProto)
    .build();
FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileDescriptorProto, new FileDescriptor[]{});
Descriptor descriptor = fileDescriptor.findMessageTypeByName("AirQuality");

// Create dynamic proto stream
ProtoZerobusStream<DynamicMessage> stream = sdk.streamBuilder(tableName)
    .clientCredentials(clientId, clientSecret)
    .dynamicProto(descriptor)
    .build()
    .join();

try {
    for (int i = 0; i < 1000; i++) {
        DynamicMessage record = DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("device_name"), "sensor-" + i)
            .setField(descriptor.findFieldByName("temp"), 20 + (i % 15))
            .build();

        stream.ingest(record);
    }
    stream.flush();
} finally {
    stream.close();
}
```

### JSON Stream

For JSON record ingestion (no protobuf required):

```java
JsonZerobusStream stream = sdk.streamBuilder(tableName)
    .clientCredentials(clientId, clientSecret)
    .maxInflightRequests(10000)
    .json()
    .build()
    .join();

try {
    for (int i = 0; i < 1000; i++) {
        String jsonRecord = String.format(
            "{\"device_name\": \"sensor-%d\", \"temp\": %d, \"humidity\": %d}",
            i % 10, 20 + (i % 15), 50 + (i % 40)
        );

        stream.ingest(jsonRecord);  // Type-safe: only String accepted
    }
    stream.flush();
} finally {
    stream.close();
}
```

**Key differences from proto ingestion:**
- Use `.json()` builder method instead of `.compiledProto()` or `.dynamicProto()`
- Returns `JsonZerobusStream` instead of `ProtoZerobusStream<T>`
- Pass JSON `String` to `ingest()` instead of protobuf `Message`
- JSON must match the table schema defined in Unity Catalog
- No protobuf schema or compilation required

### Batch Ingestion

For improved throughput, use `ingestBatch()` to send multiple records in a single request:

```java
// Batch ingestion with proto records
List<AirQuality> batch = new ArrayList<>();
for (int i = 0; i < 100; i++) {
    batch.add(AirQuality.newBuilder()
        .setDeviceName("sensor-" + i)
        .setTemp(20 + (i % 15))
        .setHumidity(50 + (i % 40))
        .build());
}

Long offsetId = stream.ingestBatch(MessageBatch.of(batch));
if (offsetId != null) {
    stream.waitForOffset(offsetId);  // Wait for batch acknowledgment
}
```

```java
// Batch ingestion with JSON records
List<String> jsonBatch = new ArrayList<>();
for (int i = 0; i < 100; i++) {
    jsonBatch.add(String.format(
        "{\"device_name\": \"sensor-%d\", \"temp\": %d}",
        i, 20 + (i % 15)
    ));
}

Long offsetId = jsonStream.ingestBatch(StringBatch.of(jsonBatch));
if (offsetId != null) {
    jsonStream.waitForOffset(offsetId);
}
```

**Benefits of batch ingestion:**
- Reduced network overhead - one request per batch instead of per record
- Atomic acknowledgment - entire batch succeeds or fails together
- Higher throughput for bulk data loads

### Using Custom Headers Provider

The SDK supports custom authentication strategies through the `HeadersProvider` interface. This is useful when you need custom authentication logic, want to add additional headers, or manage tokens externally.

#### Implementing Custom HeadersProvider

You can implement the `HeadersProvider` interface to create custom authentication strategies or add additional headers:

```java
public class CustomHeadersProvider implements HeadersProvider {
    private final OAuthHeadersProvider oauthProvider;

    public CustomHeadersProvider(String tableName, String workspaceId,
                                String workspaceUrl, String clientId,
                                String clientSecret) {
        this.oauthProvider = new OAuthHeadersProvider(
            tableName, workspaceId, workspaceUrl, clientId, clientSecret);
    }

    @Override
    public Map<String, String> getHeaders() throws NonRetriableException {
        // Get standard OAuth headers
        Map<String, String> headers = new HashMap<>(oauthProvider.getHeaders());

        // Add custom headers
        headers.put("x-custom-client-version", "1.0.0");
        headers.put("x-custom-environment", "production");
        headers.put("x-custom-request-id", UUID.randomUUID().toString());

        return headers;
    }
}

// Use custom provider with the builder API
HeadersProvider customProvider = new CustomHeadersProvider(
    "catalog.schema.table", "workspace-id",
    "https://your-workspace.cloud.databricks.com",
    "client-id", "client-secret"
);

ProtoZerobusStream<AirQuality> stream = sdk.streamBuilder(tableName)
    .clientCredentials(clientId, clientSecret)
    .headersProvider(customProvider)  // Custom headers provider
    .compiledProto(AirQuality.getDefaultInstance())
    .build()
    .join();
```

**Benefits of using HeadersProvider:**
- Flexible authentication strategies beyond OAuth
- Add custom headers to all gRPC requests
- Integrate with external token management systems
- Centralize authentication logic

**Note:** The `getHeaders()` method is called synchronously when creating or recreating a stream. Make sure your implementation is thread-safe if using the same provider instance across multiple streams.

## Configuration

### Stream Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `maxInflightRequests` | 50000 | Maximum number of unacknowledged requests |
| `recovery` | true | Enable automatic stream recovery |
| `recoveryTimeoutMs` | 15000 | Timeout for recovery operations (ms) |
| `recoveryBackoffMs` | 2000 | Delay between recovery attempts (ms) |
| `recoveryRetries` | 3 | Maximum number of recovery attempts |
| `flushTimeoutMs` | 300000 | Timeout for flush operations (ms) |
| `serverLackOfAckTimeoutMs` | 60000 | Server acknowledgment timeout (ms) |
| `maxMessageSizeBytes` | 10MB | Maximum message size (server enforces 10MB limit) |
| `recordType` | PROTO | Record type: `PROTO` or `JSON` |
| `offsetCallback` | None | Callback invoked on offset acknowledgment |

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

The SDK uses **unchecked exceptions** (extending `RuntimeException`), following modern Java SDK conventions (AWS SDK v2, Google Cloud, etc.). This means you don't need to declare `throws` or wrap every call in try/catch, but you can still handle exceptions when needed.

The SDK throws two types of exceptions:

- `ZerobusException`: Retriable errors (e.g., network issues, temporary server errors)
- `NonRetriableException`: Non-retriable errors (e.g., invalid credentials, missing table, schema mismatch)

```java
// Simple usage - let exceptions propagate
stream.ingest(record);  // No throws declaration needed

// Explicit error handling when needed
try {
    stream.ingest(record);
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

Main entry point for the SDK. Implements `AutoCloseable` for optional resource cleanup.

**Constructor:**
```java
ZerobusSdk(String serverEndpoint, String unityCatalogEndpoint)
```
- `serverEndpoint` - The Zerobus gRPC endpoint (e.g., `<workspace-id>.zerobus.region.cloud.databricks.com`)
- `unityCatalogEndpoint` - The Unity Catalog endpoint (your workspace URL)

**Builder:**
```java
ZerobusSdk.builder(String serverEndpoint, String unityCatalogEndpoint)
    .executor(ExecutorService executor)  // optional custom executor
    .build()
```
Creates an SDK instance with custom configuration. If no executor is provided, a cached thread pool is used.

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
<RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
    TableProperties<RecordType> tableProperties,
    String clientId,
    String clientSecret,
    StreamConfigurationOptions options,
    HeadersProvider headersProvider
)
```
Creates a new ingestion stream. If `headersProvider` is null, automatically creates an `OAuthHeadersProvider` using the provided credentials. Returns a CompletableFuture that completes when the stream is ready.

```java
<RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> recreateStream(
    ZerobusStream<RecordType> stream
)
```
Recreates a failed stream, resending unacknowledged records. Uses the same authentication method (OAuth or custom headers provider) as the original stream. Returns a CompletableFuture that completes when the stream is ready.

```java
void close()
```
Closes the SDK and releases resources (gRPC channel and thread pool). Optional - daemon threads ensure cleanup on JVM shutdown even without explicit close. Can be used with try-with-resources.

```java
StreamBuilder streamBuilder(String tableName)
```
Creates a new builder for configuring and creating a stream. This is the preferred way to create streams with compile-time type safety.

```java
static String getVersion()
```
Returns the current SDK version string (e.g., `"0.2.0"`).

**Constants:**

```java
public static final String VERSION = "0.2.0"
```
The current SDK version.

---

### StreamBuilder

Step builder for creating streams with compile-time type safety. You must call either `clientCredentials()` (for OAuth) or `unauthenticated()` (for custom auth) to get a configurable builder.

**Methods:**

```java
AuthenticatedStreamBuilder clientCredentials(String clientId, String clientSecret)
```
Sets OAuth client credentials. Returns `AuthenticatedStreamBuilder` for configuration and schema selection. This is the most common authentication method.

```java
UnauthenticatedStreamBuilder unauthenticated()
```
Returns `UnauthenticatedStreamBuilder` for custom authentication. Use `headersProvider()` to set custom authentication headers.

---

### AuthenticatedStreamBuilder / UnauthenticatedStreamBuilder

Both builders provide the same configuration and schema selection methods. The difference is that `AuthenticatedStreamBuilder` has OAuth credentials configured, while `UnauthenticatedStreamBuilder` does not.

**Configuration Methods:**

```java
headersProvider(HeadersProvider headersProvider)
```
Sets a custom headers provider for adding headers to gRPC requests.

```java
tlsConfig(TlsConfig tlsConfig)
```
Sets custom TLS configuration. Optional - defaults to secure TLS.

```java
maxInflightRequests(int maxInflightRequests)
```
Sets maximum in-flight requests. Must be positive.

```java
recovery(boolean recovery)
```
Enables/disables automatic recovery.

```java
recoveryTimeoutMs(int recoveryTimeoutMs)
```
Sets recovery timeout in milliseconds. Must be non-negative.

```java
recoveryBackoffMs(int recoveryBackoffMs)
```
Sets backoff between recovery attempts. Must be non-negative.

```java
recoveryRetries(int recoveryRetries)
```
Sets maximum recovery attempts. Must be non-negative.

```java
flushTimeoutMs(int flushTimeoutMs)
```
Sets flush timeout in milliseconds. Must be non-negative.

```java
serverLackOfAckTimeoutMs(int serverLackOfAckTimeoutMs)
```
Sets server acknowledgment timeout. Must be positive.

```java
maxMessageSizeBytes(int maxMessageSizeBytes)
```
Sets maximum message size in bytes. Must be positive.

```java
offsetCallback(LongConsumer offsetCallback)
```
Sets callback for offset acknowledgments.

**Schema Selection Methods:**

```java
<T extends Message> ProtoStreamBuilder<T> compiledProto(T defaultInstance)
```
Configures for compiled protobuf records. Returns a builder that produces `ProtoZerobusStream<T>`.

```java
ProtoStreamBuilder<DynamicMessage> dynamicProto(Descriptors.Descriptor descriptor)
```
Configures for dynamic protobuf records. Returns a builder that produces `ProtoZerobusStream<DynamicMessage>`.

```java
JsonStreamBuilder json()
```
Configures for JSON records. Returns a builder that produces `JsonZerobusStream`.

---

### ProtoZerobusStream\<T\>

Type-safe stream for protobuf record ingestion. Implements `AutoCloseable`.

**Methods:**

```java
long ingest(T record) throws ZerobusException
```
Ingests a single protobuf record. Type-safe - only accepts records of type `T`.

```java
long ingest(byte[] bytes) throws ZerobusException
```
Ingests pre-serialized protobuf bytes. Useful when receiving data from Kafka or other systems that already have serialized protobuf.

```java
Long ingestBatch(MessageBatch<T> messageBatch) throws ZerobusException
```
Ingests a batch of protobuf records. Returns offset ID or null if empty. Use `MessageBatch.of(recordsList)` to create the batch wrapper.

```java
Long ingestBatch(BytesBatch bytesBatch) throws ZerobusException
```
Ingests a batch of pre-serialized protobuf bytes. Returns offset ID or null if empty. Use `BytesBatch.of(bytesList)` to create the batch wrapper.

```java
void waitForOffset(long offset) throws ZerobusException
```
Blocks until the offset is acknowledged.

```java
void flush() throws ZerobusException
```
Flushes all pending records.

```java
void close() throws ZerobusException
```
Flushes and closes the stream.

```java
String getStreamId()
```
Returns the server-assigned stream ID.

```java
StreamState getState()
```
Returns current stream state.

---

### JsonZerobusStream

Type-safe stream for JSON record ingestion. Implements `AutoCloseable`.

**Methods:**

```java
long ingest(String jsonRecord) throws ZerobusException
```
Ingests a single JSON record string.

```java
long ingest(Map<String, ?> record) throws ZerobusException
```
Ingests a Map that is automatically serialized to JSON. Supports nested Maps, Lists, Strings, Numbers, Booleans, and null values.

```java
Long ingestBatch(StringBatch stringBatch) throws ZerobusException
```
Ingests a batch of JSON records. Returns offset ID or null if empty. Use `StringBatch.of(jsonStringsList)` to create the batch wrapper.

```java
Long ingestBatch(MapBatch mapBatch) throws ZerobusException
```
Ingests a batch of Maps, each serialized to JSON. Returns offset ID or null if empty. Use `MapBatch.of(mapsList)` to create the batch wrapper.

```java
void waitForOffset(long offset) throws ZerobusException
```
Blocks until the offset is acknowledged.

```java
void flush() throws ZerobusException
```
Flushes all pending records.

```java
void close() throws ZerobusException
```
Flushes and closes the stream.

```java
String getStreamId()
```
Returns the server-assigned stream ID.

```java
StreamState getState()
```
Returns current stream state.

---

### MessageBatch\<T\>

Wrapper for a batch of protobuf message records. Used with `ProtoZerobusStream.ingestBatch()` to provide a consistent API.

**Factory Methods:**

```java
static <T extends Message> MessageBatch<T> of(Iterable<T> records)
```
Creates a MessageBatch from an iterable of protobuf messages.

```java
static <T extends Message> MessageBatch<T> of(T... records)
```
Creates a MessageBatch from varargs protobuf messages.

**Example:**
```java
List<MyRecord> records = new ArrayList<>();
records.add(MyRecord.newBuilder().setField("value1").build());
records.add(MyRecord.newBuilder().setField("value2").build());
Long offset = stream.ingestBatch(MessageBatch.of(records));
```

---

### BytesBatch

Wrapper for a batch of pre-serialized protobuf byte arrays. Used with `ProtoZerobusStream.ingestBatch()` to provide a consistent API.

**Factory Methods:**

```java
static BytesBatch of(Iterable<byte[]> bytes)
```
Creates a BytesBatch from an iterable of byte arrays.

```java
static BytesBatch of(byte[]... bytes)
```
Creates a BytesBatch from varargs byte arrays.

**Example:**
```java
List<byte[]> serializedRecords = getSerializedRecordsFromKafka();
Long offset = stream.ingestBatch(BytesBatch.of(serializedRecords));
```

---

### StringBatch

Wrapper for a batch of string records. Used with `JsonZerobusStream.ingestBatch()` to provide a consistent API.

**Factory Methods:**

```java
static StringBatch of(Iterable<String> records)
```
Creates a StringBatch from an iterable of strings.

```java
static StringBatch of(String... records)
```
Creates a StringBatch from varargs strings.

**Example:**
```java
List<String> records = new ArrayList<>();
records.add("{\"name\": \"Alice\", \"age\": 30}");
records.add("{\"name\": \"Bob\", \"age\": 25}");
Long offset = stream.ingestBatch(StringBatch.of(records));
```

---

### MapBatch

Wrapper for a batch of Map records to be serialized as JSON. Used with `JsonZerobusStream.ingestBatch()` to provide a consistent API.

**Factory Methods:**

```java
static MapBatch of(Iterable<Map<String, ?>> maps)
```
Creates a MapBatch from an iterable of Maps.

```java
static MapBatch of(Map<String, ?>... maps)
```
Creates a MapBatch from varargs Maps.

**Example:**
```java
List<Map<String, Object>> records = new ArrayList<>();
records.add(createRecord("sensor-1", 25));
records.add(createRecord("sensor-2", 30));
Long offset = stream.ingestBatch(MapBatch.of(records));
```

---

### HeadersProvider

Interface for providing custom headers for gRPC stream authentication and configuration.

**Methods:**

```java
Map<String, String> getHeaders() throws NonRetriableException
```
Returns headers to be attached to gRPC requests. Called when creating or recreating a stream.

---

### OAuthHeadersProvider

Default implementation of `HeadersProvider` that uses OAuth 2.0 Client Credentials flow with Unity Catalog privileges.

**Constructor:**
```java
OAuthHeadersProvider(
    String tableName,
    String workspaceId,
    String workspaceUrl,
    String clientId,
    String clientSecret
)
```
- `tableName` - Fully qualified table name (catalog.schema.table)
- `workspaceId` - Databricks workspace ID
- `workspaceUrl` - Unity Catalog endpoint URL
- `clientId` - OAuth client ID
- `clientSecret` - OAuth client secret

**Methods:**

```java
Map<String, String> getHeaders() throws NonRetriableException
```
Fetches a fresh OAuth token and returns headers containing authorization and table name.

---

### ZerobusStream\<RecordType\>

Represents an active ingestion stream.

**Methods:**

```java
long ingest(Object record) throws ZerobusException
```
Ingests a single record into the stream. Accepts both protobuf `Message` objects and JSON `String` objects based on stream configuration. Returns the logical offset ID immediately. Use `waitForOffset(long)` to wait for server acknowledgment.

```java
Long ingestBatch(Iterable<?> records) throws ZerobusException
```
Ingests a batch of records into the stream. All records must match the stream's configured record type. The batch is assigned a single offset ID and acknowledged atomically. Returns the offset ID for the batch, or `null` if empty.

```java
void waitForOffset(long offset) throws ZerobusException
```
Blocks until the specified offset is acknowledged by the server. Use with `ingest()`/`ingestBatch()` for explicit acknowledgment control.

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
StreamConfigurationOptionsBuilder setMaxInflightRequests(int maxInflightRecords)
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
StreamConfigurationOptionsBuilder setMaxMessageSizeBytes(int maxMessageSizeBytes)
```
Sets the maximum message size in bytes. Default is 10MB (`DEFAULT_MAX_MESSAGE_SIZE_BYTES`). Server enforces 10MB limit - messages exceeding this will be rejected.

```java
StreamConfigurationOptionsBuilder setRecordType(RecordType recordType)
```
Sets the record type for the stream: `RecordType.PROTO` (default) for protobuf records, or `RecordType.JSON` for JSON records.

```java
StreamConfigurationOptionsBuilder setOffsetCallback(LongConsumer offsetCallback)
```
Sets a callback to be invoked when records are acknowledged. The callback receives the durability offset ID.

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

Base exception for retriable errors. Extends `RuntimeException` (unchecked), so no `throws` declaration is required.

**Constructors:**
```java
ZerobusException(String message)
ZerobusException(String message, Throwable cause)
```

---

### NonRetriableException

Exception for non-retriable errors such as invalid credentials, missing table, or schema mismatch. Extends `ZerobusException` (and therefore also `RuntimeException`).

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
7. **Proto generation**: Use the Zerobus CLI tool (`zerobus-cli-0.1.0.jar`) to automatically generate proto files from your table schemas
