package com.databricks.zerobus;

import java.util.concurrent.CompletableFuture;

/**
 * Result of an asynchronous record ingestion operation.
 *
 * <p>This class provides two futures that track different stages of the ingestion process:
 * <ul>
 *   <li><b>recordAccepted</b>: Completes when the SDK has queued the record internally</li>
 *   <li><b>writeCompleted</b>: Completes when the server has durably written the record</li>
 * </ul>
 *
 * <p>If either future fails, the record may not have been successfully ingested. However,
 * due to network timing, it's possible the server received and stored the record even if
 * the acknowledgment was lost.
 *
 * <p>Example usage:
 * <pre>{@code
 * IngestRecordResult result = stream.ingestRecord(myRecord);
 *
 * // Wait for SDK to accept the record (fast)
 * result.getRecordAccepted().join();
 *
 * // Wait for durable storage (slower)
 * result.getWriteCompleted().join();
 * }</pre>
 */
public class IngestRecordResult {
  private final CompletableFuture<Void> recordAccepted;
  private final CompletableFuture<Void> writeCompleted;

  /**
   * Creates a new IngestRecordResult.
   *
   * @param recordAccepted Future that completes when the SDK accepts the record
   * @param writeCompleted Future that completes when the record is durably written
   */
  public IngestRecordResult(CompletableFuture<Void> recordAccepted, CompletableFuture<Void> writeCompleted) {
    this.recordAccepted = recordAccepted;
    this.writeCompleted = writeCompleted;
  }

  /**
   * Returns a future that completes when the SDK accepts the record for processing.
   *
   * <p>This typically completes quickly, only waiting for queue space availability.
   * Completion indicates the record is queued but not yet sent to the server.
   *
   * @return Future that completes when the record is accepted by the SDK
   */
  public CompletableFuture<Void> getRecordAccepted() {
    return recordAccepted;
  }

  /**
   * Returns a future that completes when the record is durably written to storage.
   *
   * <p>This completes after the server acknowledges successful storage. This is the
   * stronger guarantee and may take longer depending on server load and network latency.
   *
   * @return Future that completes when the record is durably stored
   */
  public CompletableFuture<Void> getWriteCompleted() {
    return writeCompleted;
  }
}
