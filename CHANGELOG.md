# Version changelog

## Release v0.2.0

### New Features and Improvements

- **New ingestion API**: New stream classes have ingestion methods that return offset directly
  - All `ingest()` and `ingestBatch()` methods return `long` offset ID
  - Returns immediately after SDK accepts the record
  - Use `waitForOffset(long)` to wait for server acknowledgment if needed
  - Legacy `ZerobusStream.ingestRecord()` still returns `CompletableFuture<Void>` for backwards compatibility
  - `ZerobusStream.ingestRecord()` is deprecated and will be changed to also return a `long` offset ID in a future relase
- **Wait for Offset**: Added `waitForOffset(long offset)` method
  - Blocks until the specified offset is acknowledged by the server
  - Enables fine-grained control over durability guarantees
  - Use with `ingest()` / `ingestBatch()` for explicit acknowledgment control
- **Batch API**: Consistent `ingestBatch()` API using wrapper types
  - Batch interface hierarchy: `Batch<R>` base interface, `PrimaryBatch<P>` and `SecondaryBatch<S>` marker interfaces
  - All batch classes in `com.databricks.zerobus.batch` package
  - `ProtoZerobusStream.ingestBatch(PrimaryBatch<T>)` / `ingestBatch(MessageBatch<T>)` for proto message batches
  - `ProtoZerobusStream.ingestBatch(BytesBatch)` for pre-serialized bytes batches
  - `JsonZerobusStream.ingestBatch(PrimaryBatch<Map<String, ?>>)` / `ingestBatch(MapBatch)` for Map batches
  - `JsonZerobusStream.ingestBatch(StringBatch)` for JSON string batches
  - Wrapper types (`MessageBatch`, `BytesBatch`, `StringBatch`, `MapBatch`) provide consistent API and avoid Java type erasure issues
  - Batch is assigned a single offset ID and acknowledged atomically
  - Returns the offset ID directly for explicit acknowledgment control
- **JSON Record Support**: Added `JsonZerobusStream` for ingesting JSON records directly
  - Use `sdk.streamBuilder(tableName).json()` to create a JSON stream
  - Type-safe `ingest(String jsonRecord)` method accepts JSON strings
  - JSON records are sent directly to the server without protobuf encoding
  - Simplifies integration when records are already in JSON format
- **Type Widening for Ingest Methods**: Extended `ingest()` and `ingestBatch()` to accept multiple input types
  - `ProtoZerobusStream.ingest(byte[])` accepts pre-serialized protobuf bytes directly
  - `JsonZerobusStream.ingest(Map<String, ?>)` accepts Maps that are serialized to JSON automatically
  - Useful when receiving pre-serialized data from Kafka or other systems
- **SDK Builder Pattern**: New builder for creating `ZerobusSdk` instances
  - `ZerobusSdk.builder(serverEndpoint, unityCatalogEndpoint).executor(customExecutor).build()`
  - Optional custom `ExecutorService` for thread management
  - Original constructor still works
- **Stream Builder Pattern**: New `streamBuilder()` API for creating streams with compile-time type safety
  - `sdk.streamBuilder(tableName).clientCredentials(clientId, clientSecret).compiledProto(MyRecord.getDefaultInstance()).build()`
  - Two authentication paths: `clientCredentials(id, secret)` for OAuth, `unauthenticated()` for custom auth
  - Three schema selection methods: `compiledProto(T defaultInstance)`, `dynamicProto(Descriptor descriptor)`, `json()`
  - Returns typed streams: `ProtoZerobusStream<T>` for proto or `JsonZerobusStream` for JSON
  - **Compile-time enforcement**: `clientCredentials()` returns `AuthenticatedStreamBuilder`, `unauthenticated()` returns `UnauthenticatedStreamBuilder`. Schema methods only exist on these builders, so attempting to call `compiledProto()` directly on `StreamBuilder` results in a compile error.
  - All stream configuration options available on the builder (recovery, timeouts, headersProvider, callbacks, etc.)
- **Extended Proto Type Support**: Added support for additional Unity Catalog types in proto generation
  - `TINYINT` / `BYTE` -> `int32`
  - `TIMESTAMP_NTZ` -> `int64`
  - `VARIANT` -> `string` (unshredded JSON string)
  - `STRUCT<field1:type1, field2:type2>` -> nested protobuf message
    - Field names are converted to PascalCase for message names (e.g., `user_location` -> `UserLocation`)
    - Nested STRUCTs are fully supported (STRUCT within STRUCT)
    - `ARRAY<STRUCT<...>>` generates repeated nested messages
    - `MAP<key, STRUCT<...>>` generates map with nested message values
- **Unchecked Exceptions**: `ZerobusException` now extends `RuntimeException`
  - Follows modern Java SDK conventions (AWS SDK v2, Google Cloud, etc.)
  - No longer requires `throws` declarations or mandatory try/catch
  - Simplifies client code while still allowing exception handling when needed
  - `NonRetriableException` also becomes unchecked (as subclass of `ZerobusException`)
- **Type-Safe Stream Classes**: New strongly-typed stream classes replace generic `ZerobusStream`
  - `BaseZerobusStream<P>` abstract base class with primary record type
  - `DualTypeStream<P, S>` extends base class with secondary record type support
  - `ProtoZerobusStream<T>` extends `DualTypeStream<T, byte[]>` for compiled and dynamic protobuf schemas
  - `JsonZerobusStream` extends `DualTypeStream<Map<String, ?>, String>` for JSON record ingestion
  - Type-safe `ingest(P record)` methods prevent runtime type errors
  - Secondary type methods (`ingest(S)`) for raw/pre-serialized data
  - Generic `ZerobusStream<T>` still available for backwards compatibility
- **TableProperties Hierarchy**: Simplified table properties class hierarchy for different schema types
  - `BaseTableProperties` - abstract base class with `tableName` field
  - `ProtoTableProperties` - concrete class storing `descriptorProto` directly with factory methods:
    - `ProtoTableProperties.fromCompiled(tableName, defaultInstance)` - for compiled .proto schemas
    - `ProtoTableProperties.fromDynamic(tableName, descriptor)` - for runtime-created descriptors
  - `JsonTableProperties` - for JSON streams without protobuf schema
  - `TableProperties<T>` is deprecated in favor of `ProtoTableProperties`
- **Configuration Method Renames**: Renamed configuration methods for clarity
  - `maxInflightRequests()` replaces `maxInflightRecords()` (deprecated)
  - `setMaxInflightRequests()` replaces `setMaxInflightRecords()` (deprecated)
  - `offsetCallback()` replaces `ackCallback()` (deprecated)
  - `setOffsetCallback()` replaces `setAckCallback()` (deprecated)
- **Graceful Close Signal Handling**: Improved handling of server-initiated stream close signals
  - When the server sends a `CloseStreamSignal` with a duration, the SDK now waits for pending acknowledgments up to the specified duration before triggering stream recovery
  - This reduces duplicate records when the server gracefully closes a stream, as records that are acknowledged during the grace period won't be resent
  - If all inflight records are acknowledged before the timeout, recovery proceeds with no duplicates
  - Logs clearly indicate whether all records were acked or if recovery is proceeding with pending records
- **Headers Provider Support**: Added `HeadersProvider` interface for flexible authentication strategies
  - New `HeadersProvider` interface allows custom authentication implementations
  - New `OAuthHeadersProvider` class provides OAuth 2.0 authentication with Unity Catalog privileges
  - Enhanced `createStream()` method with optional `headersProvider` parameter
  - When `headersProvider` is null, automatically creates `OAuthHeadersProvider` (default OAuth behavior)
  - Support for adding custom headers to all gRPC requests
  - Simplified API: single `createStream()` method for both OAuth and custom authentication
  - `recreateStream()` automatically uses the same authentication method as the original stream
- **TLS Configuration Support**: Added `TlsConfig` abstract class for flexible TLS settings
  - New `TlsConfig` abstract class defines TLS configuration strategies
  - New `SecureTlsConfig` class provides secure TLS with system CA certificates (default)
  - Enhanced `createStream()` method with optional `tlsConfig` parameter
  - When `tlsConfig` is null, automatically uses `SecureTlsConfig` (TLS with system CAs)
  - `recreateStream()` automatically uses the same TLS configuration as the original stream
- **User-Agent Header**: Added user-agent header for SDK version tracking
  - `OAuthHeadersProvider` includes `user-agent: zerobus-sdk-java/<version>` header
  - Sent during stream creation for server-side SDK usage tracking
- **Configurable Message Size Limit**: Added `maxMessageSizeBytes` option in `StreamConfigurationOptions`
  - Default is 10MB (`DEFAULT_MAX_MESSAGE_SIZE_BYTES`) - matches server limit
  - Messages exceeding 10MB will be rejected by the server
- **gRPC Channel Reuse**: gRPC channel is now cached and reused across all streams
  - Reduces connection overhead when creating multiple streams
  - Single HTTP/2 connection multiplexed across streams
  - Improved performance for high-throughput applications
- **SDK-Level Thread Pool**: Changed from per-stream fixed thread pool to SDK-level cached thread pool
  - Threads created on-demand and reused across streams
  - Idle threads automatically terminated after 60 seconds
  - Scales naturally with number of active streams
- **AutoCloseable Support**: `ZerobusSdk` and all stream types implement `AutoCloseable`
  - Can be used with try-with-resources for automatic cleanup
  - `close()` shuts down gRPC channel and thread pool
  - Optional - daemon threads ensure cleanup on JVM shutdown even without explicit close
  - Streams (`ProtoZerobusStream`, `JsonZerobusStream`) also implement `AutoCloseable`
- **Stream Recreation**: New `recreate(ZerobusSdk)` method on stream classes
  - `ProtoZerobusStream.recreate(sdk)` returns `CompletableFuture<ProtoZerobusStream<T>>`
  - `JsonZerobusStream.recreate(sdk)` returns `CompletableFuture<JsonZerobusStream>`
  - Creates a new stream with the same configuration and re-ingests unacknowledged records
  - Replaces the deprecated `ZerobusSdk.recreateStream()` method
- **Dependency Shading**: Shaded JAR now relocates all dependencies to avoid classpath conflicts
  - Protobuf relocated to `com.databricks.zerobus.shaded.protobuf`
  - gRPC relocated to `com.databricks.zerobus.shaded.grpc`
  - Guava relocated to `com.databricks.zerobus.shaded.guava`
  - Perfmark relocated to `com.databricks.zerobus.shaded.perfmark`
  - Error Prone annotations relocated to `com.databricks.zerobus.shaded.errorprone`
  - Google API protos relocated to `com.databricks.zerobus.shaded.google.*`
  - Prevents conflicts when host application uses different versions of these libraries
- **Input Validation**: Comprehensive input validation in builders
  - `StreamBuilder` validates all parameters (non-null, positive values, etc.)
  - Clear error messages indicate which parameter is invalid
  - Validation happens at configuration time, not at stream creation
- **Graceful Executor Shutdown**: SDK now waits for in-flight tasks during shutdown
  - `close()` waits up to 5 seconds for tasks to complete gracefully
  - Falls back to forced shutdown if tasks don't complete in time
  - Preserves the interrupted status of the calling thread
- **SDK Version Accessor**: New methods to access SDK version programmatically
  - `ZerobusSdk.VERSION` constant (e.g., `"0.2.0"`)
  - `ZerobusSdk.getVersion()` static method
  - Useful for logging, debugging, and compatibility checks
- **Parameterized Logging**: All internal logging uses SLF4J parameterized style
  - Uses `logger.debug("Message: {}", value)` instead of string concatenation
  - Improves performance when logging is disabled (no string allocation)
- **Nullability Annotations**: Public APIs annotated with `@Nonnull` and `@Nullable` (JSR-305)
  - Helps IDE provide better warnings and suggestions
  - Improves code documentation and static analysis
  - Uses `com.google.code.findbugs:jsr305` for annotation definitions
- Updated Protocol Buffers from 3.24.0 to 4.33.0 for improved performance and latest features
- Updated gRPC dependencies from 1.58.0 to 1.76.0 for enhanced stability and security
- Updated SLF4J logging framework from 1.7.36 to 2.0.17 for modern logging capabilities

### Bug Fixes

### Documentation

- Reorganized examples into `proto/` and `json/` subdirectories for better organization
- Added single record and batch record examples for both proto and JSON formats
- Added documentation for custom authentication and TLS configuration in examples README
- Updated README.md with new dependency versions
- Updated protoc compiler version recommendations
- Updated Logback version compatibility for SLF4J 2.0
- Updated type mapping documentation with new supported types (TINYINT, BYTE, TIMESTAMP_NTZ, VARIANT, STRUCT)

### Internal Changes

- **Multi-Module Project Structure**: Reorganized into a multi-module Maven project
  - Parent POM at root level with `common/`, `sdk/`, and `cli/` modules
  - `common/` - Shared utilities (HTTP client and JSON parsing)
  - `sdk/` - SDK source code
  - `cli/` - Standalone command-line tools
- **New CLI Module**: Separate command-line tool module (`zerobus-cli`)
  - Version 0.1.0 - independent versioning from SDK
  - Standalone shaded JAR with all dependencies bundled
  - Entry point: `com.databricks.zerobus.cli.Main`
  - `GenerateProto` tool moved from SDK to CLI module (`com.databricks.zerobus.cli.GenerateProto`)
  - Currently supports `generate-proto` command for proto schema generation
  - Run via: `java -jar zerobus-cli-0.1.0.jar <command> [options]`
- **SDK JAR Cleanup**: Removed `Main-Class` from SDK shaded JAR manifest
  - SDK JAR is now a pure library without executable entry point
  - Use CLI JAR (`zerobus-cli-0.1.0.jar`) for command-line tools instead
- **Simplified Table Properties**: Removed intermediate classes
  - Deleted `CompiledProtoTableProperties` and `DynamicProtoTableProperties`
  - Unified into single `ProtoTableProperties` class with factory methods
- **Stream Config Simplification**: Removed internal config wrapper classes
  - `ProtoStreamConfig` and `JsonStreamConfig` removed
  - Stream classes now use `config` directly from base class
- Updated maven-compiler-plugin from 3.11.0 to 3.14.1
- All gRPC artifacts now consistently use version 1.76.0
- Reorganized test suite into separate test classes by functionality
- Added base test class for shared test infrastructure
- Changed Dependabot schedule from daily to monthly

### API Changes

**New APIs**

- **Type-Safe Stream Classes**: New `ProtoZerobusStream<T>` and `JsonZerobusStream` with type-safe `ingest()` methods
  - `ProtoZerobusStream.ingest(T)` returns `long` offset ID directly
  - `JsonZerobusStream.ingest(String)` returns `long` offset ID directly
  - Legacy `ZerobusStream.ingestRecord()` still available and returns `CompletableFuture<Void>`
  - Migration (optional): Replace `stream.ingestRecord(record).join()` with `long offset = stream.ingest(record);`

- **Batch Ingestion**: New `ingestBatch()` methods on typed stream classes using wrapper types
  - `ProtoZerobusStream.ingestBatch(MessageBatch<T>)` / `ingestBatch(BytesBatch)` returns `Long` offset ID (or `null` if empty)
  - `JsonZerobusStream.ingestBatch(StringBatch)` / `ingestBatch(MapBatch)` returns `Long` offset ID (or `null` if empty)

- **Type Widening**: Additional `ingest()` overloads for flexible input types
  - `ProtoZerobusStream.ingest(byte[])` for pre-serialized protobuf bytes
  - `JsonZerobusStream.ingest(Map<String, ?>)` for Map input (auto-serialized to JSON)

- **Batch Wrapper Types**: Consistent `ingestBatch()` API using wrapper types
  - `Batch<R>` - base interface for all batch types
  - `PrimaryBatch<P>` - marker interface for primary type batches
  - `SecondaryBatch<S>` - marker interface for secondary type batches
  - `MessageBatch<T>.of(Iterable<T>)` / `MessageBatch.of(T...)` - wrapper for proto message batches (implements `PrimaryBatch<T>`)
  - `BytesBatch.of(Iterable<byte[]>)` / `BytesBatch.of(byte[]...)` - wrapper for byte array batches (implements `SecondaryBatch<byte[]>`)
  - `StringBatch.of(Iterable<String>)` / `StringBatch.of(String...)` - wrapper for JSON string batches (implements `SecondaryBatch<String>`)
  - `MapBatch.of(Iterable<Map<String, ?>>)` / `MapBatch.of(Map<String, ?>...)` - wrapper for Map batches (implements `PrimaryBatch<Map<String, ?>>`)

- **Stream Recreation Methods**: New `recreate()` method on stream classes
  - `ProtoZerobusStream.recreate(ZerobusSdk)` - recreates proto stream with same config
  - `JsonZerobusStream.recreate(ZerobusSdk)` - recreates JSON stream with same config
  - Automatically re-ingests unacknowledged records from the original stream
  - Returns `CompletableFuture` with the new stream instance

- **Stream Class Hierarchy**: New abstract base classes for type-safe streams
  - `BaseZerobusStream<P>` - base class with primary type parameter
  - `DualTypeStream<P, S>` - extends base with secondary type parameter
  - All batch types are now in `com.databricks.zerobus.batch` package

**Deprecations**

- **ZerobusSdk Constructor**: `new ZerobusSdk(serverEndpoint, unityCatalogEndpoint)` is deprecated
  - Use `ZerobusSdk.builder(serverEndpoint, unityCatalogEndpoint).build()` instead

- **ZerobusStream Class**: `ZerobusStream<T>` is deprecated
  - Use `ProtoZerobusStream<T>` instead via `ZerobusSdk.streamBuilder(String)`
  - All methods on `ZerobusStream` are also deprecated:
    - `ingestRecord()` - use `ingest()` instead, which returns the offset ID directly
    - `getConfig()` - use `streamBuilder()` to create new streams instead
    - Constructor - use `ZerobusSdk.streamBuilder(String)` instead

- **ZerobusSdk.createStream()**: All `createStream()` overloads are deprecated
  - Use `ZerobusSdk.streamBuilder(String)` instead
  - Deprecated overloads:
    - `createStream(TableProperties, clientId, clientSecret, options, headersProvider, tlsConfig)`
    - `createStream(TableProperties, clientId, clientSecret, options)`
    - `createStream(TableProperties, clientId, clientSecret)`

- **ZerobusSdk.recreateStream()**: `recreateStream(ZerobusStream)` is deprecated
  - Use `streamBuilder()` to create a new stream and manually re-ingest unacknowledged records

- **Callback Methods**: `setAckCallback()` / `ackCallback()` are deprecated
  - Use `setOffsetCallback(LongConsumer)` / `offsetCallback()` instead
  - Callbacks now receive the offset ID directly (`long`) instead of the full protobuf response

- **Configuration Methods**:
  - `maxInflightRecords()` is deprecated - use `maxInflightRequests()` instead
  - `setMaxInflightRecords()` is deprecated - use `setMaxInflightRequests()` instead

- **TableProperties**: `TableProperties<T>` is deprecated
  - Use `ProtoTableProperties.fromCompiled(tableName, defaultInstance)` for compiled .proto schemas
  - Use `ProtoTableProperties.fromDynamic(tableName, descriptor)` for runtime-created protobuf descriptors
  - Use `JsonTableProperties` for JSON streams without protobuf schema

**Breaking Changes**

- **Protocol Buffers 4.x Migration**: If you use the regular JAR (not the shaded JAR), you must upgrade to protobuf-java 4.33.0 and regenerate any custom `.proto` files using protoc 4.x
  - Download protoc 4.33.0 from: https://github.com/protocolbuffers/protobuf/releases/tag/v33.0
  - Regenerate proto files: `protoc --java_out=src/main/java src/main/proto/record.proto`
  - Protobuf 4.x is binary-compatible over the wire with 3.x, but generated Java code may differ

- **SLF4J 2.0 Migration**: If you use a logging implementation, you may need to update it:
  - `slf4j-simple`: Use version 2.0.17 or later
  - `logback-classic`: Use version 1.4.14 or later (for SLF4J 2.0 compatibility)
  - `log4j-slf4j-impl`: Use version 2.20.0 or later

**Note**: If you use the shaded JAR (classifier `shaded`), all dependencies are bundled and relocated, so no action is required.

## Release v0.1.0

Initial release of the Databricks Zerobus Ingest SDK for Java.

### API Changes

- Added `ZerobusSdk` class for creating ingestion streams
- Added `ZerobusStream` class for managing stateful gRPC streams
- Added `RecordAcknowledgment` for blocking until record acknowledgment
- Added `TableProperties` for configuring table schema and name
- Added `StreamConfigurationOptions` for stream behavior configuration
- Added `ZerobusException` and `NonRetriableException` for error handling
- Added `StreamState` enum for tracking stream lifecycle
- Added utility methods in `ZerobusSdkStubUtils` for gRPC stub management
- Support for Java 8 and higher
