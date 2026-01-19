package com.databricks.zerobus;

import com.databricks.zerobus.stream.BaseZerobusStream;
import com.databricks.zerobus.stream.GrpcErrorHandling;
import com.databricks.zerobus.stream.ZerobusStream;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry point for the Zerobus SDK.
 *
 * <p>This class provides methods to create and recreate streams for ingesting records into
 * Databricks tables.
 *
 * <p>Example usage with builder:
 *
 * <pre>{@code
 * ZerobusSdk sdk = ZerobusSdk.builder("server-endpoint.databricks.com",
 *                                     "https://workspace.databricks.com")
 *     .executor(customExecutor)
 *     .build();
 *
 * ZerobusStream<MyRecord> stream = sdk.createStream(
 *     tableProperties,
 *     clientId,
 *     clientSecret,
 *     options
 * ).join();
 *
 * // When done with the SDK
 * sdk.close();
 * }</pre>
 *
 * <p>Or using the constructor for simple cases:
 *
 * <pre>{@code
 * ZerobusSdk sdk = new ZerobusSdk(
 *     "server-endpoint.databricks.com",
 *     "https://workspace.databricks.com"
 * );
 * }</pre>
 */
public class ZerobusSdk implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(ZerobusSdk.class);
  private static final StreamConfigurationOptions DEFAULT_OPTIONS =
      StreamConfigurationOptions.getDefault();

  /** The current version of the Zerobus SDK. */
  public static final String VERSION = "0.2.0";

  private final String serverEndpoint;
  private final String unityCatalogEndpoint;
  private final String workspaceId;
  private final ExecutorService executor;
  private ZerobusSdkStubFactory stubFactory;

  /**
   * Creates a new ZerobusSdk instance with default settings.
   *
   * <p>Uses a cached thread pool that creates threads as needed and reuses idle threads.
   *
   * @param serverEndpoint The gRPC endpoint URL for the Zerobus service
   * @param unityCatalogEndpoint The Unity Catalog endpoint URL
   */
  public ZerobusSdk(@Nonnull String serverEndpoint, @Nonnull String unityCatalogEndpoint) {
    this(
        Objects.requireNonNull(serverEndpoint, "serverEndpoint cannot be null"),
        Objects.requireNonNull(unityCatalogEndpoint, "unityCatalogEndpoint cannot be null"),
        createDefaultExecutor(),
        new ZerobusSdkStubFactory());
  }

  /**
   * Creates a new ZerobusSdk instance with custom configuration.
   *
   * <p>This constructor is package-private and intended for use by {@link ZerobusSdkBuilder}.
   *
   * @param serverEndpoint The gRPC endpoint URL for the Zerobus service
   * @param unityCatalogEndpoint The Unity Catalog endpoint URL
   * @param executor Custom executor service
   * @param stubFactory Custom stub factory
   */
  ZerobusSdk(
      @Nonnull String serverEndpoint,
      @Nonnull String unityCatalogEndpoint,
      @Nonnull ExecutorService executor,
      @Nonnull ZerobusSdkStubFactory stubFactory) {
    this.serverEndpoint = serverEndpoint;
    this.unityCatalogEndpoint = unityCatalogEndpoint;
    this.executor = executor;
    this.stubFactory = stubFactory;
    this.workspaceId = extractWorkspaceId(serverEndpoint);
  }

  /**
   * Creates a new builder for configuring a ZerobusSdk instance.
   *
   * @param serverEndpoint The gRPC endpoint URL for the Zerobus service
   * @param unityCatalogEndpoint The Unity Catalog endpoint URL
   * @return A new ZerobusSdkBuilder instance
   * @see ZerobusSdkBuilder
   */
  @Nonnull
  public static ZerobusSdkBuilder builder(
      @Nonnull String serverEndpoint, @Nonnull String unityCatalogEndpoint) {
    return new ZerobusSdkBuilder(serverEndpoint, unityCatalogEndpoint);
  }

  private static String extractWorkspaceId(String endpoint) {
    String clean = endpoint;
    if (clean.startsWith("https://")) clean = clean.substring(8);
    else if (clean.startsWith("http://")) clean = clean.substring(7);
    int dot = clean.indexOf('.');
    return dot > 0 ? clean.substring(0, dot) : clean;
  }

  /** Creates the default executor service. Package-private for use by {@link ZerobusSdkBuilder}. */
  static ExecutorService createDefaultExecutor() {
    ThreadFactory factory =
        new ThreadFactory() {
          private final AtomicInteger counter = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("ZerobusSdk-worker-" + counter.getAndIncrement());
            return t;
          }
        };
    return Executors.newCachedThreadPool(factory);
  }

  /**
   * Creates a stream with the specified options.
   *
   * @param tableProperties Configuration for the target table
   * @param clientId OAuth client ID
   * @param clientSecret OAuth client secret
   * @param options Stream configuration options
   * @return CompletableFuture that completes with the stream
   * @deprecated Since 0.2.0. Use {@link #streamBuilder(String)} instead. This method will be
   *     removed in a future release.
   */
  @Deprecated
  @Nonnull
  public <RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
      @Nonnull TableProperties<RecordType> tableProperties,
      @Nonnull String clientId,
      @Nonnull String clientSecret,
      @Nonnull StreamConfigurationOptions options) {
    Objects.requireNonNull(tableProperties, "tableProperties cannot be null");
    Objects.requireNonNull(clientId, "clientId cannot be null");
    Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
    Objects.requireNonNull(options, "options cannot be null");

    logger.debug("Creating stream for table: {}", tableProperties.getTableName());

    return streamBuilder(tableProperties.getTableName())
        .clientCredentials(clientId, clientSecret)
        .options(options)
        .legacyProto(tableProperties)
        .build()
        .exceptionally(
            e -> {
              throw new RuntimeException(wrapException(e));
            });
  }

  /**
   * Creates a stream with default options.
   *
   * @param tableProperties Configuration for the target table
   * @param clientId OAuth client ID
   * @param clientSecret OAuth client secret
   * @return CompletableFuture that completes with the stream
   * @deprecated Since 0.2.0. Use {@link #streamBuilder(String)} instead. This method will be
   *     removed in a future release.
   */
  @Deprecated
  @Nonnull
  public <RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
      @Nonnull TableProperties<RecordType> tableProperties,
      @Nonnull String clientId,
      @Nonnull String clientSecret) {
    Objects.requireNonNull(tableProperties, "tableProperties cannot be null");
    Objects.requireNonNull(clientId, "clientId cannot be null");
    Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
    return createStream(
        tableProperties, clientId, clientSecret, StreamConfigurationOptions.getDefault());
  }

  // ==================== Builder API ====================

  /**
   * Creates a builder for configuring and creating a stream.
   *
   * <p>This is the preferred way to create streams. Use the builder to specify the schema type and
   * configure stream options.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * // Compiled proto stream.
   * ProtoZerobusStream<MyRecord> stream = sdk.streamBuilder("catalog.schema.table")
   *     .clientCredentials(clientId, clientSecret)
   *     .compiledProto(MyRecord.getDefaultInstance())
   *     .maxInflightRequests(10000)
   *     .build()
   *     .join();
   *
   * // JSON stream.
   * JsonZerobusStream stream = sdk.streamBuilder("catalog.schema.table")
   *     .clientCredentials(clientId, clientSecret)
   *     .json()
   *     .build()
   *     .join();
   * }</pre>
   *
   * @param tableName The fully qualified table name (catalog.schema.table)
   * @return A new ZerobusStreamBuilder instance
   */
  @Nonnull
  public ZerobusStreamBuilder streamBuilder(@Nonnull String tableName) {
    Objects.requireNonNull(tableName, "tableName cannot be null");
    return new ZerobusStreamBuilder(
        stubFactory, executor, workspaceId, serverEndpoint, unityCatalogEndpoint, tableName);
  }

  /**
   * Recreates a stream from a failed stream.
   *
   * <p>Creates a new stream with the same configuration and re-ingests any unacknowledged records
   * from the failed stream. This method works with all stream types ({@link
   * com.databricks.zerobus.stream.ProtoZerobusStream}, {@link
   * com.databricks.zerobus.stream.JsonZerobusStream}, etc.).
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * try {
   *     stream.ingest(record);
   * } catch (ZerobusException e) {
   *     // Stream failed, recreate it
   *     ProtoZerobusStream<MyRecord> newStream = sdk.recreateStream(stream).join();
   *     // Continue using newStream
   * }
   * }</pre>
   *
   * @param <S> The stream type
   * @param failedStream The stream to recreate
   * @return CompletableFuture that completes with a new stream of the same type
   */
  @Nonnull
  @SuppressWarnings("unchecked")
  public <S extends BaseZerobusStream<?>> CompletableFuture<S> recreateStream(
      @Nonnull S failedStream) {
    Objects.requireNonNull(failedStream, "failedStream cannot be null");
    return (CompletableFuture<S>) failedStream.recreate(this);
  }

  /**
   * Recreates a legacy stream from a failed stream.
   *
   * <p>Uses the same configuration and re-ingests unacknowledged records.
   *
   * @param failedStream The stream to recreate
   * @return CompletableFuture that completes with the new stream
   * @deprecated Since 0.2.0. Use {@link #recreateStream(BaseZerobusStream)} instead. This method
   *     will be removed in a future release.
   */
  @Deprecated
  @Nonnull
  public <RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> recreateStream(
      @Nonnull ZerobusStream<RecordType> failedStream) {
    Objects.requireNonNull(failedStream, "failedStream cannot be null");

    CompletableFuture<ZerobusStream<RecordType>> result = new CompletableFuture<>();

    TableProperties<RecordType> tableProperties = failedStream.getLegacyTableProperties();

    streamBuilder(tableProperties.getTableName())
        .clientCredentials(failedStream.getClientId(), failedStream.getClientSecret())
        .options(failedStream.getOptions())
        .headersProvider(failedStream.getHeadersProvider())
        .tlsConfig(failedStream.getTlsConfig())
        .legacyProto(tableProperties)
        .build()
        .whenComplete(
            (stream, error) -> {
              if (error != null) {
                result.completeExceptionally(error);
                return;
              }
              try {
                Iterator<RecordType> unacked = failedStream.getUnackedRecords();
                while (unacked.hasNext()) {
                  stream.ingestRecord(unacked.next());
                }
                result.complete(stream);
              } catch (ZerobusException e) {
                result.completeExceptionally(e);
              }
            });
    return result;
  }

  /**
   * Closes the SDK and releases resources.
   *
   * <p>This method performs a graceful shutdown:
   *
   * <ol>
   *   <li>Shuts down the gRPC channel
   *   <li>Stops accepting new tasks on the executor
   *   <li>Waits up to 5 seconds for in-flight tasks to complete
   *   <li>Forces shutdown if tasks don't complete in time
   * </ol>
   *
   * <p>After calling this method, the SDK instance cannot be reused. Create a new instance if
   * needed.
   *
   * <p>Note: If using daemon threads (the default), resources will also be cleaned up automatically
   * on JVM shutdown, so calling close() is optional but recommended for explicit resource
   * management.
   */
  @Override
  public void close() {
    logger.debug("Closing ZerobusSdk");
    stubFactory.shutdown();
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
        logger.warn("Executor did not terminate gracefully, forcing shutdown");
        executor.shutdownNow();
        if (!executor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS)) {
          logger.error("Executor did not terminate after forced shutdown");
        }
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupted while waiting for executor shutdown");
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns the current version of the Zerobus SDK.
   *
   * <p>This can be used for logging, debugging, or verifying SDK compatibility:
   *
   * <pre>{@code
   * System.out.println("Using Zerobus SDK version: " + ZerobusSdk.getVersion());
   * }</pre>
   *
   * @return The SDK version string (e.g., "0.2.0")
   */
  public static String getVersion() {
    return VERSION;
  }

  private Throwable wrapException(Throwable e) {
    if (e instanceof ZerobusException) return e;
    if (e instanceof StatusRuntimeException) {
      Status.Code code = ((StatusRuntimeException) e).getStatus().getCode();
      if (GrpcErrorHandling.isNonRetriable(code)) {
        return new NonRetriableException("Non-retriable gRPC error: " + e.getMessage(), e);
      }
    }
    return new ZerobusException("Failed to create stream: " + e.getMessage(), e);
  }
}
