package com.databricks.zerobus.stream;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/**
 * Tracks a batch of records that have been queued for sending but not yet acknowledged.
 *
 * <p>This class stores both the original records (for {@code getUnackedRecords()}) and the encoded
 * batch (for resending during recovery).
 *
 * <p>For single record ingestion, this holds a batch of size 1.
 *
 * @param <P> The primary record type (Message for proto, Map for JSON)
 */
final class InflightBatch<P> {

  /**
   * Original records in this batch, for returning via {@code getUnackedRecords()}.
   *
   * <p>May be null if records were ingested in serialized form (e.g., {@code ingest(byte[])} or
   * {@code ingest(String)}).
   */
  @Nullable final List<P> records;

  /** Encoded batch payload for sending/resending over the wire. */
  final EncodedBatch encodedBatch;

  /**
   * Logical offset ID for this batch.
   *
   * <p>This is mutable because during stream recovery, batches need to be reassigned new offset IDs
   * when they are re-sent to the server.
   */
  long offsetId;

  /** Promise that completes when the server acknowledges this batch. */
  final CompletableFuture<Long> ackPromise;

  /**
   * Creates a new inflight batch.
   *
   * @param records Original records (null if ingested as serialized data)
   * @param encodedBatch Encoded payload for transmission
   * @param offsetId Logical offset ID
   * @param ackPromise Promise to complete on acknowledgment
   */
  InflightBatch(
      @Nullable List<P> records,
      EncodedBatch encodedBatch,
      long offsetId,
      CompletableFuture<Long> ackPromise) {
    this.records = records;
    this.encodedBatch = encodedBatch;
    this.offsetId = offsetId;
    this.ackPromise = ackPromise;
  }

  /** Returns the number of records in this batch. */
  int size() {
    return encodedBatch.size();
  }
}
