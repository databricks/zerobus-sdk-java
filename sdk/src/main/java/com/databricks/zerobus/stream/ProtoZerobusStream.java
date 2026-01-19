package com.databricks.zerobus.stream;

import com.databricks.zerobus.RecordType;
import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusGrpc.ZerobusStub;
import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.ZerobusStreamBuilder;
import com.databricks.zerobus.auth.HeadersProvider;
import com.databricks.zerobus.batch.PrimaryBatch;
import com.databricks.zerobus.batch.proto.BytesBatch;
import com.databricks.zerobus.batch.proto.MessageBatch;
import com.databricks.zerobus.schema.ProtoTableProperties;
import com.databricks.zerobus.tls.TlsConfig;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Zerobus stream for ingesting protobuf records into a table.
 *
 * <p>This stream type is used for both compiled protobuf schemas and dynamic schemas. The type
 * parameter {@code T} represents the protobuf message type being ingested.
 *
 * <p>Example usage with compiled schema:
 *
 * <pre>{@code
 * ProtoZerobusStream<MyRecord> stream = sdk.streamBuilder("catalog.schema.table")
 *     .clientCredentials(clientId, clientSecret)
 *     .compiledProto(MyRecord.getDefaultInstance())
 *     .build()
 *     .join();
 *
 * MyRecord record = MyRecord.newBuilder()
 *     .setField("value")
 *     .build();
 * stream.ingest(record);
 * stream.close();
 * }</pre>
 *
 * <p>Example usage with dynamic schema:
 *
 * <pre>{@code
 * ProtoZerobusStream<DynamicMessage> stream = sdk.streamBuilder("catalog.schema.table")
 *     .clientCredentials(clientId, clientSecret)
 *     .dynamicProto(descriptor)
 *     .build()
 *     .join();
 *
 * DynamicMessage record = DynamicMessage.newBuilder(descriptor)
 *     .setField(field, "value")
 *     .build();
 * stream.ingest(record);
 * stream.close();
 * }</pre>
 *
 * @param <T> The protobuf message type (primary type)
 * @see JsonZerobusStream
 * @see DualTypeStream
 * @see ZerobusSdk#streamBuilder(String)
 */
public class ProtoZerobusStream<T extends Message> extends DualTypeStream<T, byte[]> {

  // ==================== Constructor ====================

  /**
   * Creates a new ProtoZerobusStream.
   *
   * <p>Use {@link ZerobusSdk#streamBuilder(String)} instead of calling this constructor directly.
   */
  public ProtoZerobusStream(
      Supplier<ZerobusStub> stubSupplier,
      ProtoTableProperties tableProperties,
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
        executor,
        tableProperties.getDescriptorProto());
  }

  @Override
  @Nonnull
  protected RecordType getRecordType() {
    return RecordType.PROTO;
  }

  // ==================== Public API ====================

  /**
   * Ingests a protobuf record into the stream (primary type).
   *
   * @param record The protobuf record to ingest
   * @return The logical offset ID assigned to this record
   * @throws ZerobusException if ingestion fails
   */
  @Override
  public long ingest(@Nonnull T record) throws ZerobusException {
    if (record == null) {
      throw new ZerobusException("Record cannot be null");
    }
    synchronized (this) {
      try {
        return doIngestRecord(record);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted while enqueuing record", e);
      }
    }
  }

  /**
   * Ingests a pre-serialized protobuf record into the stream (secondary type).
   *
   * <p>Use this method when you have pre-serialized protobuf bytes (e.g., from Kafka, another
   * system, or manual serialization). The bytes are sent directly without additional processing.
   *
   * @param bytes The serialized protobuf bytes to ingest
   * @return The logical offset ID assigned to this record
   * @throws ZerobusException if ingestion fails
   */
  // Note: This is the secondary type ingest method (S = byte[])
  public long ingest(@Nonnull byte[] bytes) throws ZerobusException {
    if (bytes == null) {
      throw new ZerobusException("Bytes cannot be null");
    }
    synchronized (this) {
      try {
        return doIngestBytes(bytes);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted while enqueuing record", e);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>For protobuf streams, use {@link MessageBatch#of(Iterable)} to create the batch.
   */
  @Override
  @Nullable public Long ingestBatch(@Nonnull PrimaryBatch<T> batch) throws ZerobusException {
    if (batch == null) {
      throw new ZerobusException("Batch cannot be null");
    }
    if (!batch.iterator().hasNext()) {
      return null;
    }
    synchronized (this) {
      try {
        return doIngestBatch(batch);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted while enqueuing batch", e);
      }
    }
  }

  /**
   * Ingests a batch of protobuf records into the stream (primary type).
   *
   * <p>This is a convenience method that accepts {@link MessageBatch} directly.
   *
   * <p>Example:
   *
   * <pre>{@code
   * List<MyRecord> records = new ArrayList<>();
   * records.add(MyRecord.newBuilder().setField("value1").build());
   * records.add(MyRecord.newBuilder().setField("value2").build());
   * Long offset = stream.ingestBatch(MessageBatch.of(records));
   * }</pre>
   *
   * @param messageBatch The batch of protobuf records to ingest
   * @return The offset ID for the batch, or null if empty
   * @throws ZerobusException if ingestion fails
   */
  @Nullable public Long ingestBatch(@Nonnull MessageBatch<T> messageBatch) throws ZerobusException {
    if (messageBatch == null) {
      throw new ZerobusException("Message batch cannot be null");
    }
    if (!messageBatch.iterator().hasNext()) {
      return null;
    }
    synchronized (this) {
      try {
        return doIngestBatch(messageBatch);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted while enqueuing batch", e);
      }
    }
  }

  /**
   * Ingests a batch of pre-serialized protobuf records into the stream.
   *
   * <p>The batch is assigned a single offset ID and acknowledged atomically.
   *
   * <p>Example:
   *
   * <pre>{@code
   * List<byte[]> serializedRecords = getSerializedRecordsFromKafka();
   * Long offset = stream.ingestBatch(BytesBatch.of(serializedRecords));
   * }</pre>
   *
   * @param bytesBatch The batch of serialized protobuf byte arrays to ingest
   * @return The offset ID for the batch, or null if empty
   * @throws ZerobusException if ingestion fails
   */
  @Nullable public Long ingestBatch(@Nonnull BytesBatch bytesBatch) throws ZerobusException {
    if (bytesBatch == null) {
      throw new ZerobusException("Bytes batch cannot be null");
    }
    if (!bytesBatch.iterator().hasNext()) {
      return null;
    }
    synchronized (this) {
      try {
        return doIngestBytesBatch(bytesBatch);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted while enqueuing batch", e);
      }
    }
  }

  // getUnackedRecords() is now implemented in BaseZerobusStream using LandingZone

  /**
   * Returns the table properties for this stream.
   *
   * @return The proto table properties
   */
  @Nonnull
  public ProtoTableProperties getTableProperties() {
    return (ProtoTableProperties) tableProperties;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Recreates this proto stream with the same configuration. Any unacknowledged records from
   * this stream will be re-ingested into the new stream.
   */
  @Override
  @Nonnull
  @SuppressWarnings("unchecked")
  public CompletableFuture<ProtoZerobusStream<T>> recreate(ZerobusSdk sdk) {
    Iterator<T> unackedRecords = getUnackedRecords();
    ProtoTableProperties tableProperties = getTableProperties();

    ZerobusStreamBuilder.AuthenticatedZerobusStreamBuilder builder =
        sdk.streamBuilder(tableProperties.getTableName())
            .clientCredentials(clientId, clientSecret)
            .options(options)
            .headersProvider(headersProvider)
            .tlsConfig(tlsConfig);

    return builder
        .<T>compiledProto((T) tableProperties.getDefaultInstance())
        .build()
        .thenApply(
            newStream -> {
              // Re-ingest unacked records
              while (unackedRecords.hasNext()) {
                try {
                  newStream.ingest(unackedRecords.next());
                } catch (ZerobusException e) {
                  throw new RuntimeException(
                      "Failed to re-ingest record during stream recreation", e);
                }
              }
              return newStream;
            });
  }

  // ==================== Internal Implementation ====================

  private long doIngestRecord(T record) throws ZerobusException, InterruptedException {
    checkIngestState();

    long offsetId = getNextOffsetId();
    ByteString encoded = ByteString.copyFrom(record.toByteArray());

    // Create batch with single record
    EncodedBatch encodedBatch = EncodedBatch.protoSingle(encoded);
    InflightBatch<T> batch =
        new InflightBatch<>(
            java.util.Collections.singletonList(record),
            encodedBatch,
            offsetId,
            new CompletableFuture<>());

    addBatch(batch);
    return offsetId;
  }

  private Long doIngestBatch(Iterable<T> records) throws ZerobusException, InterruptedException {
    checkIngestState();

    long offsetId = getNextOffsetId();
    List<ByteString> encoded = new ArrayList<>();
    List<T> recordList = new ArrayList<>();

    for (T record : records) {
      encoded.add(ByteString.copyFrom(record.toByteArray()));
      recordList.add(record);
    }

    // Create batch with multiple records
    EncodedBatch encodedBatch = EncodedBatch.protoBatch(encoded);
    InflightBatch<T> batch =
        new InflightBatch<>(recordList, encodedBatch, offsetId, new CompletableFuture<>());

    addBatch(batch);
    return offsetId;
  }

  private long doIngestBytes(byte[] bytes) throws ZerobusException, InterruptedException {
    checkIngestState();

    long offsetId = getNextOffsetId();
    ByteString encoded = ByteString.copyFrom(bytes);

    // Create batch with single encoded record (no original record stored)
    EncodedBatch encodedBatch = EncodedBatch.protoSingle(encoded);
    InflightBatch<T> batch =
        new InflightBatch<>(null, encodedBatch, offsetId, new CompletableFuture<>());

    addBatch(batch);
    return offsetId;
  }

  private Long doIngestBytesBatch(Iterable<byte[]> bytesList)
      throws ZerobusException, InterruptedException {
    checkIngestState();

    long offsetId = getNextOffsetId();
    List<ByteString> encoded = new ArrayList<>();

    for (byte[] bytes : bytesList) {
      encoded.add(ByteString.copyFrom(bytes));
    }

    // Create batch with multiple encoded records (no original records stored)
    EncodedBatch encodedBatch = EncodedBatch.protoBatch(encoded);
    InflightBatch<T> batch =
        new InflightBatch<>(null, encodedBatch, offsetId, new CompletableFuture<>());

    addBatch(batch);
    return offsetId;
  }

  // enqueueRecordsForResending() is no longer needed - recovery is handled by LandingZone
}
