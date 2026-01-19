package com.databricks.zerobus.stream;

import com.databricks.zerobus.RecordType;
import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusGrpc.ZerobusStub;
import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.ZerobusStreamBuilder;
import com.databricks.zerobus.auth.HeadersProvider;
import com.databricks.zerobus.batch.PrimaryBatch;
import com.databricks.zerobus.batch.json.MapBatch;
import com.databricks.zerobus.batch.json.StringBatch;
import com.databricks.zerobus.common.json.Json;
import com.databricks.zerobus.schema.JsonTableProperties;
import com.databricks.zerobus.tls.TlsConfig;
import com.google.protobuf.DescriptorProtos;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Zerobus stream for ingesting JSON records into a table.
 *
 * <p>This stream type is used when your data is already in JSON format and you don't want to define
 * a protobuf schema. JSON records are sent directly to the server as strings.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * JsonZerobusStream stream = sdk.streamBuilder("catalog.schema.table")
 *     .clientCredentials(clientId, clientSecret)
 *     .json()
 *     .build()
 *     .join();
 *
 * String jsonRecord = "{\"field\": \"value\", \"count\": 42}";
 * stream.ingest(jsonRecord);
 * stream.close();
 * }</pre>
 *
 * @see ProtoZerobusStream
 * @see DualTypeStream
 * @see ZerobusSdk#streamBuilder(String)
 */
public class JsonZerobusStream extends DualTypeStream<Map<String, ?>, String> {

  // Empty descriptor proto for JSON streams (required by server but not used for schema)
  private static final DescriptorProtos.DescriptorProto EMPTY_DESCRIPTOR =
      DescriptorProtos.DescriptorProto.newBuilder().setName("JsonRecord").build();

  // ==================== Constructor ====================

  /**
   * Creates a new JsonZerobusStream.
   *
   * <p>Use {@link ZerobusSdk#streamBuilder(String)} instead of calling this constructor directly.
   */
  public JsonZerobusStream(
      Supplier<ZerobusStub> stubSupplier,
      JsonTableProperties tableProperties,
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
        EMPTY_DESCRIPTOR);
  }

  @Override
  @Nonnull
  protected RecordType getRecordType() {
    return RecordType.JSON;
  }

  // ==================== Public API ====================

  /**
   * Ingests a JSON record into the stream.
   *
   * @param jsonRecord The JSON string to ingest
   * @return The logical offset ID assigned to this record
   * @throws ZerobusException if ingestion fails or the record is not a valid JSON string
   */
  public long ingest(@Nonnull String jsonRecord) throws ZerobusException {
    if (jsonRecord == null) {
      throw new ZerobusException("JSON record cannot be null");
    }
    synchronized (this) {
      try {
        return doIngestRecord(jsonRecord);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted while enqueuing record", e);
      }
    }
  }

  /**
   * Ingests a record as a Map into the stream (primary type).
   *
   * <p>The Map is serialized to JSON before being sent to the server. The original Map is stored
   * internally for recovery via {@link #getUnackedRecords()}.
   *
   * <p>Supported value types:
   *
   * <ul>
   *   <li>{@code Map<String, ?>} - nested objects
   *   <li>{@code Iterable<?>} - arrays
   *   <li>{@code String}, {@code Number}, {@code Boolean}, {@code null}
   * </ul>
   *
   * @param record The Map to ingest (will be serialized to JSON)
   * @return The logical offset ID assigned to this record
   * @throws ZerobusException if ingestion fails or serialization fails
   */
  @Override
  public long ingest(@Nonnull Map<String, ?> record) throws ZerobusException {
    if (record == null) {
      throw new ZerobusException("Record cannot be null");
    }
    String jsonRecord;
    try {
      jsonRecord = Json.stringify(record);
    } catch (IllegalArgumentException e) {
      throw new ZerobusException("Failed to serialize Map to JSON: " + e.getMessage(), e);
    }
    synchronized (this) {
      try {
        return doIngestMapRecord(record, jsonRecord);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted while enqueuing record", e);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>For JSON streams, use {@link MapBatch#of(Iterable)} to create the batch. The original Maps
   * are stored internally for recovery via {@link #getUnackedRecords()}.
   */
  @Override
  @Nullable public Long ingestBatch(@Nonnull PrimaryBatch<Map<String, ?>> batch) throws ZerobusException {
    if (batch == null) {
      throw new ZerobusException("Batch cannot be null");
    }
    if (!batch.iterator().hasNext()) {
      return null;
    }
    // Convert Maps to JSON strings while keeping original Maps for recovery
    List<Map<String, ?>> records = new ArrayList<>();
    List<String> jsonStrings = new ArrayList<>();
    try {
      for (Map<String, ?> record : batch) {
        records.add(record);
        jsonStrings.add(Json.stringify(record));
      }
    } catch (IllegalArgumentException e) {
      throw new ZerobusException("Failed to serialize Map to JSON: " + e.getMessage(), e);
    }
    synchronized (this) {
      try {
        return doIngestMapBatch(records, jsonStrings);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted while enqueuing batch", e);
      }
    }
  }

  /**
   * Ingests a batch of JSON records into the stream (secondary type).
   *
   * <p>The batch is assigned a single offset ID and acknowledged atomically.
   *
   * <p>Example:
   *
   * <pre>{@code
   * List<String> records = new ArrayList<>();
   * records.add("{\"name\": \"Alice\", \"age\": 30}");
   * records.add("{\"name\": \"Bob\", \"age\": 25}");
   * Long offset = stream.ingestBatch(StringBatch.of(records));
   * }</pre>
   *
   * @param stringBatch The batch of JSON strings to ingest
   * @return The offset ID for the batch, or null if empty
   * @throws ZerobusException if ingestion fails
   */
  @Nullable public Long ingestBatch(@Nonnull StringBatch stringBatch) throws ZerobusException {
    if (stringBatch == null) {
      throw new ZerobusException("String batch cannot be null");
    }
    if (!stringBatch.iterator().hasNext()) {
      return null;
    }
    synchronized (this) {
      try {
        return doIngestBatch(stringBatch);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted while enqueuing batch", e);
      }
    }
  }

  /**
   * Ingests a batch of records as Maps into the stream (primary type).
   *
   * <p>This is a convenience method that accepts {@link MapBatch} directly.
   *
   * <p>Example:
   *
   * <pre>{@code
   * List<Map<String, Object>> records = new ArrayList<>();
   * records.add(createRecord("sensor-1", 25));
   * records.add(createRecord("sensor-2", 30));
   * Long offset = stream.ingestBatch(MapBatch.of(records));
   * }</pre>
   *
   * @param mapBatch The batch of Maps to ingest (each will be serialized to JSON)
   * @return The offset ID for the batch, or null if empty
   * @throws ZerobusException if ingestion fails or serialization fails
   */
  @Nullable public Long ingestBatch(@Nonnull MapBatch mapBatch) throws ZerobusException {
    return ingestBatch((PrimaryBatch<Map<String, ?>>) mapBatch);
  }

  // getUnackedRecords() is now implemented in BaseZerobusStream using LandingZone
  // When Maps are ingested, they are stored in InflightBatch.records and can be retrieved

  /**
   * Returns the table properties for this stream.
   *
   * @return The JSON table properties
   */
  @Nonnull
  public JsonTableProperties getTableProperties() {
    return (JsonTableProperties) tableProperties;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Recreates this JSON stream with the same configuration. Any unacknowledged records from this
   * stream will be re-ingested into the new stream.
   */
  @Override
  @Nonnull
  public CompletableFuture<JsonZerobusStream> recreate(ZerobusSdk sdk) {
    Iterator<Map<String, ?>> unackedRecords = getUnackedRecords();
    JsonTableProperties tableProperties = getTableProperties();

    ZerobusStreamBuilder.AuthenticatedZerobusStreamBuilder builder =
        sdk.streamBuilder(tableProperties.getTableName())
            .clientCredentials(clientId, clientSecret)
            .options(options)
            .headersProvider(headersProvider)
            .tlsConfig(tlsConfig);

    return builder
        .json()
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

  private long doIngestRecord(String jsonRecord) throws ZerobusException, InterruptedException {
    checkIngestState();

    long offsetId = getNextOffsetId();

    // Create batch with single JSON record (no original Map stored for String ingestion)
    EncodedBatch encodedBatch = EncodedBatch.jsonSingle(jsonRecord);
    InflightBatch<Map<String, ?>> batch =
        new InflightBatch<>(null, encodedBatch, offsetId, new CompletableFuture<>());

    addBatch(batch);
    return offsetId;
  }

  /** Internal method to ingest a Map record with the original Map stored for recovery. */
  private long doIngestMapRecord(Map<String, ?> record, String jsonRecord)
      throws ZerobusException, InterruptedException {
    checkIngestState();

    long offsetId = getNextOffsetId();

    // Create batch with single JSON record and store original Map for recovery
    EncodedBatch encodedBatch = EncodedBatch.jsonSingle(jsonRecord);
    InflightBatch<Map<String, ?>> batch =
        new InflightBatch<>(
            java.util.Collections.singletonList(record),
            encodedBatch,
            offsetId,
            new CompletableFuture<>());

    addBatch(batch);
    return offsetId;
  }

  private Long doIngestBatch(Iterable<String> jsonRecords)
      throws ZerobusException, InterruptedException {
    checkIngestState();

    long offsetId = getNextOffsetId();
    List<String> jsonList = new ArrayList<>();

    for (String record : jsonRecords) {
      jsonList.add(record);
    }

    // Create batch with multiple JSON records (no original Maps stored)
    EncodedBatch encodedBatch = EncodedBatch.jsonBatch(jsonList);
    InflightBatch<Map<String, ?>> batch =
        new InflightBatch<>(null, encodedBatch, offsetId, new CompletableFuture<>());

    addBatch(batch);
    return offsetId;
  }

  /** Internal method to ingest a batch of Map records with original Maps stored for recovery. */
  private Long doIngestMapBatch(List<Map<String, ?>> records, List<String> jsonRecords)
      throws ZerobusException, InterruptedException {
    checkIngestState();

    long offsetId = getNextOffsetId();

    // Create batch with multiple JSON records and store original Maps for recovery
    EncodedBatch encodedBatch = EncodedBatch.jsonBatch(jsonRecords);
    InflightBatch<Map<String, ?>> batch =
        new InflightBatch<>(records, encodedBatch, offsetId, new CompletableFuture<>());

    addBatch(batch);
    return offsetId;
  }

  // enqueueRecordsForResending() is no longer needed - recovery is handled by LandingZone
}
