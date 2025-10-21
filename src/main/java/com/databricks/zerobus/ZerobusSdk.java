package com.databricks.zerobus;

import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry point for the Zerobus SDK.
 *
 * <p>This class provides methods to create and recreate streams for ingesting records into
 * Databricks tables. It handles authentication, connection management, and stream lifecycle
 * operations.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ZerobusSdk sdk = new ZerobusSdk(
 *     "server-endpoint.databricks.com",
 *     "https://workspace.databricks.com"
 * );
 *
 * ZerobusStream<MyRecord> stream = sdk.createStream(
 *     tableProperties,
 *     clientId,
 *     clientSecret,
 *     options
 * ).join();
 * }</pre>
 *
 * @see ZerobusStream
 * @see StreamConfigurationOptions
 */
public class ZerobusSdk {
  private static final Logger logger = LoggerFactory.getLogger(ZerobusSdk.class);

  // Constants
  private static final StreamConfigurationOptions DEFAULT_OPTIONS =
      StreamConfigurationOptions.getDefault();
  private static final int STREAM_EXECUTOR_THREAD_POOL_SIZE = 4;
  private static final String HTTPS_PREFIX = "https://";
  private static final String HTTP_PREFIX = "http://";
  private static final String THREAD_NAME_PREFIX = "ZerobusStream-executor-";

  private static final Random RANDOM = new Random();

  private final String serverEndpoint;
  private final String unityCatalogEndpoint;
  private final String workspaceId;

  private ZerobusSdkStubFactory stubFactory = ZerobusSdkStubFactory.create();

  /**
   * Creates a new ZerobusSdk instance.
   *
   * @param serverEndpoint The gRPC endpoint URL for the Zerobus service.
   * @param unityCatalogEndpoint The Unity Catalog endpoint URL.
   */
  public ZerobusSdk(String serverEndpoint, String unityCatalogEndpoint) {
    this.serverEndpoint = serverEndpoint;
    this.unityCatalogEndpoint = unityCatalogEndpoint;
    this.workspaceId = extractWorkspaceId(serverEndpoint);
  }

  /**
   * Sets the stub factory (used for testing).
   *
   * @param stubFactory The stub factory to use
   */
  void setStubFactory(ZerobusSdkStubFactory stubFactory) {
    this.stubFactory = stubFactory;
  }

  /**
   * Extracts workspace ID from server endpoint.
   *
   * <p>The workspace ID is the first component of the endpoint hostname.
   *
   * <p>Example: {@code 1234567890123456.zerobus.us-west-2.cloud.databricks.com} returns {@code
   * 1234567890123456}
   *
   * @param endpoint The server endpoint (may include protocol prefix)
   * @return The extracted workspace ID
   */
  private static String extractWorkspaceId(String endpoint) {
    String cleanEndpoint = endpoint;

    // Remove protocol prefix if present
    if (cleanEndpoint.startsWith(HTTPS_PREFIX)) {
      cleanEndpoint = cleanEndpoint.substring(HTTPS_PREFIX.length());
    } else if (cleanEndpoint.startsWith(HTTP_PREFIX)) {
      cleanEndpoint = cleanEndpoint.substring(HTTP_PREFIX.length());
    }

    // Extract workspace ID (first part before first dot)
    int dotIndex = cleanEndpoint.indexOf('.');
    return dotIndex > 0 ? cleanEndpoint.substring(0, dotIndex) : cleanEndpoint;
  }

  /**
   * Creates an executor service for stream operations.
   *
   * <p>The executor uses daemon threads to avoid preventing JVM shutdown. Each thread is named with
   * a unique instance ID for debugging purposes.
   *
   * @return A new ExecutorService configured for stream operations
   */
  private static ExecutorService createStreamExecutor() {
    long instanceId = 1000000000L + Math.abs(RANDOM.nextLong() % 9000000000L);

    ThreadFactory daemonThreadFactory =
        new ThreadFactory() {
          private final AtomicInteger counter = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            thread.setName(THREAD_NAME_PREFIX + instanceId + "-" + counter.getAndIncrement());
            return thread;
          }
        };

    return Executors.newFixedThreadPool(STREAM_EXECUTOR_THREAD_POOL_SIZE, daemonThreadFactory);
  }

  /**
   * Creates a new gRPC stream for ingesting records into a table.
   *
   * <p>Opens a stream which can be used for blocking and/or non-blocking gRPC calls to ingest data
   * to the given table. At this stage, the table and message descriptor will be validated.
   *
   * @param tableProperties Configuration for the target table including table name and record type
   *     information.
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @param options Configuration options for the stream including timeouts, retry settings, and
   *     callback functions.
   * @param <RecordType> The type of records to be ingested (must extend Message).
   * @return A CompletableFuture that completes with the ZerobusStream when the stream is ready.
   */
  public <RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
      TableProperties<RecordType> tableProperties,
      String clientId,
      String clientSecret,
      StreamConfigurationOptions options) {

    ExecutorService streamExecutor = createStreamExecutor();
    CompletableFuture<ZerobusStream<RecordType>> resultFuture = new CompletableFuture<>();

    try {
      logger.debug("Creating stream for table: " + tableProperties.getTableName());

      // Create a token supplier that generates a fresh token for each gRPC request
      java.util.function.Supplier<String> tokenSupplier =
          () -> {
            try {
              return TokenFactory.getZerobusToken(
                  tableProperties.getTableName(),
                  workspaceId,
                  unityCatalogEndpoint,
                  clientId,
                  clientSecret);
            } catch (NonRetriableException e) {
              throw new RuntimeException("Failed to get Zerobus token", e);
            }
          };

      // Create gRPC stub once with token supplier - it will fetch fresh tokens as needed
      ZerobusGrpc.ZerobusStub stub =
          stubFactory.createStubWithTokenSupplier(
              serverEndpoint, tableProperties.getTableName(), tokenSupplier);

      ZerobusStream<RecordType> stream =
          new ZerobusStream<>(
              stub,
              tableProperties,
              stubFactory,
              serverEndpoint,
              workspaceId,
              unityCatalogEndpoint,
              clientId,
              clientSecret,
              options,
              streamExecutor,
              streamExecutor);

      stream
          .initialize()
          .whenComplete(
              (result, error) -> {
                if (error == null) {
                  resultFuture.complete(stream);
                } else {
                  resultFuture.completeExceptionally(error);
                }
              });
    } catch (Throwable e) {
      logger.error("Failed to create stream with: " + e.getMessage(), e);

      Throwable ex;
      if (e instanceof ZerobusException) {
        ex = e;
      } else if (e instanceof StatusRuntimeException) {
        StatusRuntimeException sre = (StatusRuntimeException) e;
        Status.Code code = sre.getStatus().getCode();
        if (GrpcErrorHandling.isNonRetriable(code)) {
          ex =
              new NonRetriableException(
                  "Non-retriable gRPC error during stream creation: " + sre.getMessage(), sre);
        } else {
          ex = new ZerobusException("Failed to create stream: " + sre.getMessage(), sre);
        }
      } else {
        ex = new ZerobusException("Failed to create stream: " + e.getMessage(), e);
      }
      resultFuture.completeExceptionally(ex);
    }

    return resultFuture;
  }

  /**
   * Creates a new gRPC stream for ingesting records into a table with default options.
   *
   * @param tableProperties Configuration for the target table including table name and record type
   *     information.
   * @param clientId The OAuth client ID for authentication.
   * @param clientSecret The OAuth client secret for authentication.
   * @param <RecordType> The type of records to be ingested (must extend Message).
   * @return A CompletableFuture that completes with the ZerobusStream when the stream is ready.
   */
  public <RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> createStream(
      TableProperties<RecordType> tableProperties, String clientId, String clientSecret) {
    return this.createStream(tableProperties, clientId, clientSecret, DEFAULT_OPTIONS);
  }

  /**
   * Recreate stream from a failed stream.
   *
   * <p>Uses the same table properties and stream options as the failed stream. It will also ingest
   * all unacknowledged records from the failed stream.
   *
   * @param failedStream The stream to be recreated.
   * @param <RecordType> The type of records to be ingested (must extend Message).
   * @return A CompletableFuture that completes with the new ZerobusStream when the stream is ready.
   */
  public <RecordType extends Message> CompletableFuture<ZerobusStream<RecordType>> recreateStream(
      ZerobusStream<RecordType> failedStream) {

    CompletableFuture<ZerobusStream<RecordType>> resultFuture = new CompletableFuture<>();

    createStream(
            failedStream.getTableProperties(),
            failedStream.getClientId(),
            failedStream.getClientSecret(),
            failedStream.getOptions())
        .whenComplete(
            (stream, error) -> {
              if (error == null) {
                // ingest unacked records
                Iterator<RecordType> unackedRecords = failedStream.getUnackedRecords();

                try {
                  while (unackedRecords.hasNext()) {
                    stream.ingestRecord(unackedRecords.next());
                  }
                  resultFuture.complete(stream);
                } catch (ZerobusException e) {
                  resultFuture.completeExceptionally(e);
                }
              } else {
                resultFuture.completeExceptionally(error);
              }
            });

    return resultFuture;
  }
}
