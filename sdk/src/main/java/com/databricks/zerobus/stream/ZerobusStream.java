package com.databricks.zerobus.stream;

import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.TableProperties;
import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusGrpc.ZerobusStub;
import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.auth.HeadersProvider;
import com.databricks.zerobus.tls.TlsConfig;
import com.google.protobuf.Message;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * Zerobus stream for ingesting protobuf records into a table.
 *
 * <p>This is the original stream class maintained for backwards compatibility. It extends {@link
 * ProtoZerobusStream} and provides additional backwards-compatible methods.
 *
 * <p>For new code, prefer using the fluent API:
 *
 * <pre>{@code
 * ProtoZerobusStream<MyRecord> stream = sdk.streamBuilder("catalog.schema.table")
 *     .clientCredentials(clientId, clientSecret)
 *     .protoSchema(MyRecord.getDefaultInstance())
 *     .build()
 *     .join();
 * }</pre>
 *
 * @param <RecordType> The type of the protobuf message being ingested
 * @see ProtoZerobusStream
 * @see ZerobusSdk#streamBuilder(String)
 * @deprecated Since 0.2.0. Use {@link ProtoZerobusStream} instead via {@link
 *     ZerobusSdk#streamBuilder(String)}. This class will be removed in a future release.
 */
@Deprecated
public class ZerobusStream<RecordType extends Message> extends ProtoZerobusStream<RecordType> {

  private final TableProperties<RecordType> legacyTableProperties;

  // ==================== Constructor ====================

  /**
   * Creates a new ZerobusStream.
   *
   * <p>Use {@link ZerobusSdk#streamBuilder(String)} instead of calling this constructor directly.
   *
   * @param stubSupplier Supplier for gRPC stubs
   * @param tableProperties Table configuration
   * @param clientId OAuth client ID
   * @param clientSecret OAuth client secret
   * @param headersProvider Custom headers provider (may be null)
   * @param tlsConfig TLS configuration
   * @param options Stream configuration options
   * @param executor Executor for async operations
   * @deprecated Since 0.2.0. Use {@link ZerobusSdk#streamBuilder(String)} instead. This constructor
   *     will be removed in a future release.
   */
  @Deprecated
  public ZerobusStream(
      Supplier<ZerobusStub> stubSupplier,
      TableProperties<RecordType> tableProperties,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider,
      TlsConfig tlsConfig,
      StreamConfigurationOptions options,
      ExecutorService executor) {
    super(
        stubSupplier,
        tableProperties,
        clientId,
        clientSecret,
        headersProvider,
        tlsConfig,
        options,
        executor);
    this.legacyTableProperties = tableProperties;
  }

  // ==================== Backwards-Compatible API ====================

  /**
   * Returns the table properties for this stream.
   *
   * <p>Overrides the parent method to return the original {@link TableProperties} type with generic
   * parameter, preserving backwards compatibility.
   *
   * @return The table properties
   * @deprecated Since 0.2.0. Use {@link ZerobusSdk#streamBuilder(String)} to create streams
   *     instead. This method will be removed in a future release.
   */
  @Override
  @Deprecated
  public TableProperties<RecordType> getTableProperties() {
    return legacyTableProperties;
  }

  /**
   * Returns the legacy table properties for this stream.
   *
   * <p>Alias for {@link #getTableProperties()}, used internally for stream recreation.
   *
   * @return The legacy table properties
   * @deprecated Since 0.2.0. Use {@link ZerobusSdk#streamBuilder(String)} to create streams
   *     instead. This method will be removed in a future release.
   */
  @Deprecated
  public TableProperties<RecordType> getLegacyTableProperties() {
    return legacyTableProperties;
  }

  /**
   * @deprecated Used internally for stream recreation.
   */
  @Deprecated
  public String getClientId() {
    return clientId;
  }

  /**
   * @deprecated Used internally for stream recreation.
   */
  @Deprecated
  public String getClientSecret() {
    return clientSecret;
  }

  /**
   * @deprecated Used internally for stream recreation.
   */
  @Deprecated
  public StreamConfigurationOptions getOptions() {
    return options;
  }

  /**
   * @deprecated Used internally for stream recreation.
   */
  @Deprecated
  public HeadersProvider getHeadersProvider() {
    return headersProvider;
  }

  /**
   * @deprecated Used internally for stream recreation.
   */
  @Deprecated
  public TlsConfig getTlsConfig() {
    return tlsConfig;
  }

  /**
   * Ingests a record into the stream asynchronously.
   *
   * <p>The returned future completes when the record has been durably acknowledged by the server.
   *
   * <p>This method is provided for backwards compatibility. For new code, consider using {@link
   * #ingest(Message)} which returns the offset ID directly.
   *
   * @param record The protobuf record to ingest
   * @return A future that completes when the record is acknowledged
   * @throws ZerobusException if the stream is not in a valid state for ingestion
   * @deprecated Since 0.2.0. Use {@link #ingest(Message)} instead, which returns the offset ID
   *     directly. This method will be removed in a future release.
   */
  @Deprecated
  public CompletableFuture<Void> ingestRecord(RecordType record) throws ZerobusException {
    long offsetId = ingest(record);
    return getAckFutureForOffset(offsetId);
  }

  // ==================== Internal ====================

  /**
   * Gets a future that completes when the given offset is acknowledged.
   *
   * @param offsetId The offset to wait for
   * @return A future that completes when the offset is acknowledged
   */
  private CompletableFuture<Void> getAckFutureForOffset(long offsetId) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    // Find the batch with this offset and attach to its promise
    synchronized (this) {
      for (InflightBatch<RecordType> batch : landingZone.peekAll()) {
        if (batch.offsetId == offsetId) {
          batch.ackPromise.whenComplete(
              (ack, err) -> {
                if (err != null) {
                  result.completeExceptionally(err);
                } else {
                  result.complete(null);
                }
              });
          return result;
        }
      }
    }
    // If not found in landing zone, it may already be acknowledged
    if (offsetId <= latestAckedOffsetId) {
      return CompletableFuture.completedFuture(null);
    }
    // Otherwise wait for it
    result.completeExceptionally(
        new ZerobusException("Offset " + offsetId + " not found in landing zone"));
    return result;
  }
}
