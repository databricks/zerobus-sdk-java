package com.databricks.zerobus.stream;

import com.databricks.zerobus.CloseStreamSignal;
import com.databricks.zerobus.CreateIngestStreamRequest;
import com.databricks.zerobus.EphemeralStreamRequest;
import com.databricks.zerobus.EphemeralStreamResponse;
import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.RecordType;
import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.StreamState;
import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusGrpc.ZerobusStub;
import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.auth.HeadersProvider;
import com.databricks.zerobus.batch.PrimaryBatch;
import com.databricks.zerobus.schema.BaseTableProperties;
import com.databricks.zerobus.tls.TlsConfig;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for Zerobus streams with a primary record type.
 *
 * <p>This class contains the common infrastructure for all streams, including gRPC connection
 * management, recovery logic, and acknowledgment handling.
 *
 * <p>Implements {@link AutoCloseable} to support try-with-resources:
 *
 * <pre>{@code
 * try (ProtoZerobusStream<MyRecord> stream = sdk.streamBuilder("table")
 *     .clientCredentials(clientId, clientSecret)
 *     .compiledProto(MyRecord.getDefaultInstance())
 *     .build()
 *     .join()) {
 *
 *     stream.ingest(record);
 * }
 * // Stream is automatically closed
 * }</pre>
 *
 * <p>For streams with two record types (primary and secondary), see {@link DualTypeStream}.
 *
 * @param <P> The primary record type
 * @see DualTypeStream
 * @see ProtoZerobusStream
 * @see JsonZerobusStream
 */
public abstract class BaseZerobusStream<P> implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(BaseZerobusStream.class);
  private static final int CREATE_STREAM_TIMEOUT_MS = 15000;

  // Configuration (immutable after construction)
  protected final BaseTableProperties tableProperties;
  protected final String clientId;
  protected final String clientSecret;
  protected final StreamConfigurationOptions options;
  protected final HeadersProvider headersProvider;
  protected final TlsConfig tlsConfig;
  protected final Supplier<ZerobusStub> stubSupplier;
  protected final ExecutorService executor;
  protected final DescriptorProtos.DescriptorProto descriptorProto;

  // Stream state (mutable)
  private ZerobusStub stub;
  private StreamState state = StreamState.UNINITIALIZED;
  private Optional<String> streamId = Optional.empty();
  private Optional<ClientCallStreamObserver<EphemeralStreamRequest>> grpcStream = Optional.empty();
  private Optional<CompletableFuture<String>> streamCreatedEvent = Optional.empty();

  // Batch lifecycle management
  protected final LandingZone<P> landingZone;
  private List<InflightBatch<P>> failedBatches;
  protected long latestAckedOffsetId = -1;
  protected long lastSentOffsetId = -1;

  // Background tasks and failure tracking
  private final StreamFailureInfo failureInfo = new StreamFailureInfo();
  private BackgroundTask senderTask;
  private BackgroundTask unresponsivenessTask;
  private ClientResponseObserver<EphemeralStreamRequest, EphemeralStreamResponse> ackReceiver;
  private CompletableFuture<Void> gracefulCloseTask;
  private volatile long gracefulCloseWatermark = -1;

  // ==================== Constructor ====================

  protected BaseZerobusStream(
      Supplier<ZerobusStub> stubSupplier,
      BaseTableProperties tableProperties,
      String clientId,
      String clientSecret,
      HeadersProvider headersProvider,
      TlsConfig tlsConfig,
      StreamConfigurationOptions options,
      ExecutorService executor,
      DescriptorProtos.DescriptorProto descriptorProto) {
    this.stubSupplier = stubSupplier;
    this.tableProperties = tableProperties;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.options = options;
    this.headersProvider = headersProvider;
    this.tlsConfig = tlsConfig;
    this.executor = executor;
    this.descriptorProto = descriptorProto;
    this.landingZone = new LandingZone<>(options.maxInflightRequests());

    initBackgroundTasks();
    initAckReceiver();
  }

  // ==================== Public API ====================

  /** Returns the stream ID assigned by the server. */
  public synchronized String getStreamId() {
    return streamId.orElse("");
  }

  /** Returns the current state of the stream. */
  public synchronized StreamState getState() {
    return state;
  }

  /**
   * Waits for a specific offset to be acknowledged by the server.
   *
   * @param offset The offset ID to wait for
   * @throws ZerobusException if the stream fails or times out
   */
  public void waitForOffset(long offset) throws ZerobusException {
    synchronized (this) {
      long startTime = System.currentTimeMillis();
      while (latestAckedOffsetId < offset) {
        if (state == StreamState.FAILED) {
          throw new ZerobusException("Stream failed while waiting for offset " + offset);
        }
        if (state == StreamState.CLOSED && latestAckedOffsetId < offset) {
          throw new ZerobusException("Stream closed before offset " + offset + " was acknowledged");
        }
        long remaining = options.flushTimeoutMs() - (System.currentTimeMillis() - startTime);
        if (remaining <= 0) {
          throw new ZerobusException("Timeout waiting for offset " + offset);
        }
        try {
          this.wait(remaining);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ZerobusException("Interrupted while waiting for offset " + offset, e);
        }
      }
    }
  }

  /**
   * Flushes the stream, waiting for all queued records to be acknowledged.
   *
   * @throws ZerobusException if the stream is not opened or flush fails
   */
  public void flush() throws ZerobusException {
    synchronized (this) {
      logger.debug("Flushing stream...");
      try {
        if (state == StreamState.UNINITIALIZED) {
          throw new ZerobusException("Cannot flush stream when it is not opened");
        }
        waitForRecovery();
        if (state == StreamState.OPENED) {
          setState(StreamState.FLUSHING);
        }
        waitForInflightRecords();
        logger.info("All records have been flushed");
      } finally {
        if (state == StreamState.FLUSHING) {
          setState(StreamState.OPENED);
        }
      }
    }
  }

  /**
   * Closes the stream after flushing all queued records.
   *
   * @throws ZerobusException if the stream is not opened or close fails
   */
  public void close() throws ZerobusException {
    boolean readyToClose = false;
    synchronized (this) {
      while (!readyToClose) {
        switch (state) {
          case UNINITIALIZED:
            throw new ZerobusException("Cannot close stream when it is not opened");
          case FAILED:
            throw new ZerobusException("Stream failed and cannot be gracefully closed");
          case CLOSED:
            return; // Already closed
          case FLUSHING:
          case RECOVERING:
          case PAUSED:
            try {
              this.wait();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            break;
          case OPENED:
            setState(StreamState.CLOSED);
            readyToClose = true;
            break;
        }
      }
    }

    Optional<ZerobusException> exception = Optional.empty();
    try {
      flush();
    } catch (ZerobusException ex) {
      exception = Optional.of(ex);
      throw ex;
    } catch (Exception ex) {
      ZerobusException wrapped = new ZerobusException("Flush failed during close", ex);
      exception = Optional.of(wrapped);
      throw wrapped;
    } finally {
      doCloseStream(true, exception);
    }
    logger.info("Stream gracefully closed");
  }

  // ==================== Abstract Methods (Primary Type) ====================

  /**
   * Ingests a primary record into the stream.
   *
   * @param record The record to ingest
   * @return The logical offset ID assigned to this record
   * @throws ZerobusException if ingestion fails
   */
  public abstract long ingest(@Nonnull P record) throws ZerobusException;

  /**
   * Ingests a batch of primary records into the stream.
   *
   * <p>The batch is assigned a single offset ID and acknowledged atomically.
   *
   * @param batch The batch of records to ingest
   * @return The offset ID for the batch, or null if empty
   * @throws ZerobusException if ingestion fails
   */
  @Nullable public abstract Long ingestBatch(@Nonnull PrimaryBatch<P> batch) throws ZerobusException;

  /**
   * Returns unacknowledged records after stream failure.
   *
   * <p>These records can be re-ingested when the stream is recreated. Note that records ingested as
   * raw bytes (e.g., {@code ingest(byte[])}) or raw JSON strings (e.g., {@code ingest(String)})
   * will not be included, as they don't store the original primary record.
   *
   * @return Iterator of unacknowledged primary records
   */
  @Nonnull
  public Iterator<P> getUnackedRecords() {
    if (failedBatches == null) {
      return java.util.Collections.emptyIterator();
    }
    java.util.List<P> records = new java.util.ArrayList<>();
    for (InflightBatch<P> batch : failedBatches) {
      if (batch.records != null) {
        records.addAll(batch.records);
      }
    }
    return records.iterator();
  }

  /**
   * Recreates this stream with the same configuration.
   *
   * <p>Creates a new stream and re-ingests any unacknowledged records from this stream.
   *
   * @param sdk The SDK instance to use for recreation
   * @return A future that completes with the new stream
   */
  @Nonnull
  public abstract CompletableFuture<? extends BaseZerobusStream<P>> recreate(ZerobusSdk sdk);

  /**
   * Returns the record type for this stream.
   *
   * <p>This determines how records are serialized when sent to the server.
   *
   * @return The record type (PROTO or JSON)
   */
  @Nonnull
  protected abstract RecordType getRecordType();

  // ==================== Package-private for SDK ====================

  public CompletableFuture<Void> initialize() {
    CompletableFuture<Void> result = new CompletableFuture<>();
    synchronized (this) {
      if (state != StreamState.UNINITIALIZED) {
        result.completeExceptionally(new ZerobusException("Stream already initialized"));
        return result;
      }
    }

    int retries = options.recovery() ? options.recoveryRetries() : 1;
    runWithRetries(retries, "CreateStream", this::doCreateStream)
        .whenComplete(
            (r, e) -> {
              if (e == null) {
                setState(StreamState.OPENED);
                unresponsivenessTask.start();
                logger.info("Stream created with id {}", streamId.orElse("unknown"));
                result.complete(null);
              } else {
                setState(StreamState.FAILED);
                logger.error("Failed to create stream", e);
                result.completeExceptionally(
                    e instanceof ZerobusException
                        ? e
                        : new ZerobusException("Stream creation failed", e));
              }
            });
    return result;
  }

  // ==================== Protected Methods for Subclasses ====================

  protected synchronized StreamState getStateInternal() {
    return state;
  }

  /**
   * Checks that the stream is in a valid state for ingestion.
   *
   * <p>Waits if the stream is in a transient state (RECOVERING, FLUSHING), throws if in a terminal
   * state (FAILED, CLOSED, UNINITIALIZED).
   *
   * <p>Note: Backpressure is now handled by LandingZone's semaphore, so this method no longer waits
   * for queue space.
   */
  protected void checkIngestState() throws ZerobusException, InterruptedException {
    synchronized (this) {
      while (true) {
        switch (state) {
          case RECOVERING:
          case FLUSHING:
            this.wait();
            break;
          case PAUSED:
            // Allow ingestion during PAUSED - records queue in landing zone
            // and will be sent after recovery
            return;
          case FAILED:
          case CLOSED:
          case UNINITIALIZED:
            throw new ZerobusException("Cannot ingest: stream is " + state);
          case OPENED:
            return;
        }
      }
    }
  }

  /**
   * Adds an inflight batch to the landing zone.
   *
   * <p>This method blocks if the maximum inflight capacity is reached (backpressure).
   *
   * @param batch The batch to add
   * @throws InterruptedException if interrupted while waiting for capacity
   */
  protected void addBatch(InflightBatch<P> batch) throws InterruptedException {
    landingZone.add(batch);
    synchronized (this) {
      this.notifyAll();
    }
  }

  protected long getNextOffsetId() {
    return ++lastSentOffsetId;
  }

  /**
   * Reassigns offset IDs to all batches in the landing zone.
   *
   * <p>This is called during recovery after a new stream is created. All batches (both pending and
   * those that were inflight but moved back to pending) need new offset IDs for the new stream.
   */
  private void reassignBatchOffsets() {
    synchronized (this) {
      List<InflightBatch<P>> batches = landingZone.peekAll();
      for (InflightBatch<P> batch : batches) {
        batch.offsetId = getNextOffsetId();
      }
      logger.debug("Reassigned offsets to {} batches for recovery", batches.size());
    }
  }

  // ==================== Internal Implementation ====================

  private synchronized void setState(StreamState newState) {
    state = newState;
    this.notifyAll();
    logger.debug("Stream state -> {}", newState);
  }

  private void waitForRecovery() throws ZerobusException {
    while (state == StreamState.RECOVERING) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted while waiting for recovery", e);
      }
    }
  }

  private void waitForInflightRecords() throws ZerobusException {
    long startTime = System.currentTimeMillis();
    while (landingZone.totalCount() > 0) {
      if (state == StreamState.FAILED) {
        throw new ZerobusException("Stream failed during flush");
      }
      long remaining = options.flushTimeoutMs() - (System.currentTimeMillis() - startTime);
      if (remaining <= 0) {
        throw new ZerobusException("Flush timed out");
      }
      try {
        this.wait(remaining);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZerobusException("Interrupted during flush", e);
      }
    }
  }

  // ==================== Stream Lifecycle ====================

  private CompletableFuture<Void> doCreateStream() {
    CompletableFuture<Void> result = new CompletableFuture<>();
    int timeoutMs = options.recovery() ? options.recoveryTimeoutMs() : CREATE_STREAM_TIMEOUT_MS;

    latestAckedOffsetId = -1;
    lastSentOffsetId = -1;
    streamId = Optional.empty();
    grpcStream = Optional.empty();
    streamCreatedEvent = Optional.empty();

    runWithTimeout(
            timeoutMs,
            () -> {
              CompletableFuture<Void> attempt = new CompletableFuture<>();
              stub = stubSupplier.get();
              streamCreatedEvent = Optional.of(new CompletableFuture<>());
              grpcStream =
                  Optional.of(
                      (ClientCallStreamObserver<EphemeralStreamRequest>)
                          stub.ephemeralStream(ackReceiver));

              EphemeralStreamRequest createReq =
                  EphemeralStreamRequest.newBuilder()
                      .setCreateStream(
                          CreateIngestStreamRequest.newBuilder()
                              .setTableName(tableProperties.getTableName())
                              .setDescriptorProto(
                                  ByteString.copyFrom(descriptorProto.toByteArray()))
                              .setRecordType(getRecordType())
                              .build())
                      .build();

              try {
                grpcStream.get().onNext(createReq);
              } catch (Exception e) {
                failStream(e);
                attempt.completeExceptionally(e);
                return attempt;
              }

              streamCreatedEvent
                  .get()
                  .whenComplete(
                      (id, e) -> {
                        if (e == null) {
                          streamId = Optional.of(id);
                          senderTask.start();
                          attempt.complete(null);
                        } else {
                          failStream(e);
                          streamId = Optional.empty();
                          streamCreatedEvent = Optional.empty();
                          grpcStream = Optional.empty();
                          attempt.completeExceptionally(
                              e instanceof ZerobusException
                                  ? e
                                  : new ZerobusException(e.getMessage(), e));
                        }
                      });
              return attempt;
            })
        .whenComplete(
            (r, e) -> {
              if (e == null) {
                result.complete(null);
              } else {
                failStream(e);
                Throwable ex = wrapException(e, "Stream creation failed");
                result.completeExceptionally(ex);
              }
            });
    return result;
  }

  private void failStream(Throwable error) {
    synchronized (this) {
      if (grpcStream.isPresent()) {
        try {
          grpcStream.get().onError(error);
        } catch (Exception ignored) {
        }
        grpcStream = Optional.empty();
        streamId = Optional.empty();
      }
    }
  }

  private void doCloseStream(boolean hardFailure, Optional<ZerobusException> exception) {
    synchronized (this) {
      logger.debug("Closing stream, hardFailure={}", hardFailure);
      if (hardFailure && exception.isPresent()) {
        setState(StreamState.FAILED);
      }

      senderTask.cancel();

      try {
        if (grpcStream.isPresent()) {
          grpcStream.get().onCompleted();
          if (hardFailure) {
            grpcStream.get().cancel("Stream closed", null);
          }
        }
      } catch (Exception ignored) {
      }

      if (hardFailure) {
        // Hard failure: close landing zone and fail all batches
        landingZone.close();
        unresponsivenessTask.cancel();
        // Store failed batches for getUnackedRecords()
        failedBatches = landingZone.removeAll();
        for (InflightBatch<P> batch : failedBatches) {
          batch.ackPromise.completeExceptionally(
              exception.orElse(new ZerobusException("Stream failed")));
        }
        this.notifyAll();
      } else {
        // Soft close (recovery): move inflight batches back to pending for resending
        landingZone.resetObserve();
      }

      grpcStream = Optional.empty();
      streamCreatedEvent = Optional.empty();
      streamId = Optional.empty();
      stub = null;
      this.notifyAll();
    }

    senderTask.waitUntilStopped();
    if (hardFailure) {
      unresponsivenessTask.waitUntilStopped();
    }
  }

  private CompletableFuture<Void> doCloseStreamAsync(
      boolean hardFailure, Optional<ZerobusException> exception) {
    return CompletableFuture.runAsync(() -> doCloseStream(hardFailure, exception), executor);
  }

  // ==================== Recovery ====================

  private void handleStreamFailed(StreamFailureType type, Optional<Throwable> error) {
    Optional<ZerobusException> exception =
        error.map(
            e ->
                e instanceof ZerobusException
                    ? (ZerobusException) e
                    : new ZerobusException("Stream failed: " + e.getMessage(), e));
    if (!exception.isPresent()) {
      exception = Optional.of(new ZerobusException("Stream failed"));
    }

    synchronized (this) {
      if (state == StreamState.FAILED
          || state == StreamState.UNINITIALIZED
          || state == StreamState.RECOVERING) {
        if (state == StreamState.UNINITIALIZED && streamCreatedEvent.isPresent()) {
          streamCreatedEvent.get().completeExceptionally(exception.get());
        }
        return;
      }
      if (state == StreamState.CLOSED && !error.isPresent()) {
        return;
      }
      error.ifPresent(e -> logger.error("Stream failed: {}", e.getMessage(), e));

      if (error.isPresent() && error.get() instanceof NonRetriableException) {
        doCloseStreamAsync(true, exception);
        return;
      }

      failureInfo.logFailure(type);
      setState(StreamState.RECOVERING);

      final Optional<ZerobusException> finalException = exception;
      recoverStream()
          .whenComplete(
              (r, e) -> {
                if (e == null) {
                  setState(StreamState.OPENED);
                  logger.info("Stream recovered with id {}", streamId.orElse("unknown"));
                } else {
                  logger.error("Stream recovery failed", e);
                  doCloseStream(true, finalException);
                }
              });
    }
  }

  private CompletableFuture<Void> handleStreamFailedAsync(
      StreamFailureType type, Optional<Throwable> error) {
    return CompletableFuture.runAsync(() -> handleStreamFailed(type, error), executor);
  }

  private CompletableFuture<Void> recoverStream() {
    CompletableFuture<Void> result = new CompletableFuture<>();
    CompletableFuture.runAsync(
        () -> {
          if (!options.recovery()) {
            result.completeExceptionally(new ZerobusException("Recovery is disabled"));
            return;
          }
          logger.warn("Attempting stream recovery for {}", streamId.orElse("unknown"));
          doCloseStream(false, Optional.empty());

          synchronized (this) {
            int retries = options.recoveryRetries();
            int leftRetries = Math.max(0, retries - failureInfo.getFailureCounts() + 1);
            if (leftRetries == 0) {
              result.completeExceptionally(new ZerobusException("Recovery retries exhausted"));
              return;
            }

            runWithRetries(
                    leftRetries,
                    "RecoverStream",
                    () -> {
                      CompletableFuture<Void> attempt = new CompletableFuture<>();
                      doCreateStream()
                          .whenComplete(
                              (r, e) -> {
                                if (e != null) {
                                  attempt.completeExceptionally(e);
                                } else {
                                  // Reassign offset IDs to all pending batches for resending
                                  reassignBatchOffsets();
                                  // Reset graceful close watermark
                                  gracefulCloseWatermark = -1;
                                  attempt.complete(null);
                                }
                              });
                      return attempt;
                    })
                .whenComplete(
                    (r, e) -> {
                      if (e == null) {
                        logger.info(
                            "Recovery completed, new stream id: {}", streamId.orElse("unknown"));
                        result.complete(null);
                      } else {
                        logger.error("Recovery failed", e);
                        result.completeExceptionally(e);
                      }
                    });
          }
        },
        executor);
    return result;
  }

  // ==================== Background Tasks ====================

  private void initBackgroundTasks() {
    senderTask =
        new BackgroundTask(
            token -> {
              // Check if sender should be paused (graceful close in progress)
              synchronized (this) {
                if (state == StreamState.PAUSED) {
                  try {
                    this.wait(100);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  return;
                }
              }

              // Try to observe the next batch from the landing zone
              InflightBatch<P> batch = null;
              try {
                batch = landingZone.tryObserve(100, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }

              if (batch == null) {
                // No batch available, check if we should continue waiting
                synchronized (this) {
                  if (state != StreamState.OPENED
                      && state != StreamState.FLUSHING
                      && state != StreamState.CLOSED) {
                    try {
                      this.wait(100);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                  }
                }
                return;
              }

              // Build the request from the batch
              EphemeralStreamRequest request = batch.encodedBatch.toRequest(batch.offsetId);

              if (grpcStream.isPresent()) {
                ClientCallStreamObserver<EphemeralStreamRequest> stream = grpcStream.get();
                synchronized (this) {
                  while (!stream.isReady() && !token.isDone()) {
                    try {
                      this.wait();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      break;
                    }
                  }
                }
                if (!token.isDone()) {
                  try {
                    stream.onNext(request);
                    failureInfo.resetFailure(StreamFailureType.SENDING_MESSAGE);
                  } catch (Exception e) {
                    logger.error("Error sending batch", e);
                    handleStreamFailedAsync(StreamFailureType.SENDING_MESSAGE, Optional.of(e));
                    synchronized (this) {
                      while ((state == StreamState.OPENED || state == StreamState.FLUSHING)
                          && !token.isDone()) {
                        try {
                          this.wait();
                        } catch (InterruptedException ex) {
                          Thread.currentThread().interrupt();
                          break;
                        }
                      }
                    }
                  }
                }
              }
            },
            error -> {
              logger.error("Sender task failed", error);
              doCloseStreamAsync(
                  true, Optional.of(new ZerobusException("Sender task failed", error)));
            },
            executor);

    unresponsivenessTask =
        new BackgroundTask(
            token -> {
              long startTime = System.currentTimeMillis();
              synchronized (this) {
                if (state == StreamState.UNINITIALIZED
                    || state == StreamState.CLOSED
                    || state == StreamState.FAILED) {
                  return;
                }
                if (state == StreamState.RECOVERING) {
                  try {
                    this.wait();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  return;
                }
                if (landingZone.inflightCount() == 0) {
                  try {
                    this.wait();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  return;
                }

                long prevOffset = latestAckedOffsetId;
                while (prevOffset == latestAckedOffsetId) {
                  long remaining =
                      options.serverLackOfAckTimeoutMs() - (System.currentTimeMillis() - startTime);
                  if (remaining <= 0) {
                    handleStreamFailedAsync(
                        StreamFailureType.SERVER_UNRESPONSIVE,
                        Optional.of(new ZerobusException("Server unresponsive")));
                    return;
                  }
                  try {
                    this.wait(remaining);
                    if (token.isDone()) return;
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
              }
            },
            error -> {
              logger.error("Unresponsiveness task failed", error);
              doCloseStreamAsync(
                  true, Optional.of(new ZerobusException("Unresponsiveness task failed", error)));
            },
            executor);
  }

  private void initAckReceiver() {
    ackReceiver =
        new ClientResponseObserver<EphemeralStreamRequest, EphemeralStreamResponse>() {
          private Optional<String> receiverStreamId = Optional.empty();

          @Override
          public void beforeStart(ClientCallStreamObserver<EphemeralStreamRequest> stream) {
            stream.setOnReadyHandler(
                () -> {
                  synchronized (BaseZerobusStream.this) {
                    BaseZerobusStream.this.notifyAll();
                  }
                });
          }

          @Override
          public void onNext(EphemeralStreamResponse response) {
            switch (response.getPayloadCase()) {
              case CREATE_STREAM_RESPONSE:
                String id = response.getCreateStreamResponse().getStreamId();
                if (id == null || id.isEmpty()) {
                  throw new RuntimeException(new ZerobusException("Missing stream id in response"));
                }
                receiverStreamId = Optional.of(id);
                logger.debug("Stream created with id {}", id);
                streamCreatedEvent.get().complete(id);
                break;

              case INGEST_RECORD_RESPONSE:
                String expectedId =
                    receiverStreamId.orElseThrow(
                        () ->
                            new RuntimeException(new ZerobusException("Got ack before stream id")));
                long ackedOffset = response.getIngestRecordResponse().getDurabilityAckUpToOffset();
                logger.debug("Acked offset {}", ackedOffset);

                synchronized (BaseZerobusStream.this) {
                  if (!streamId.isPresent() || !expectedId.equals(streamId.get())) {
                    return; // Stale ack from old stream
                  }
                  failureInfo.resetFailure(StreamFailureType.SERVER_CLOSED_STREAM);
                  failureInfo.resetFailure(StreamFailureType.SERVER_UNRESPONSIVE);
                  latestAckedOffsetId = Math.max(latestAckedOffsetId, ackedOffset);

                  // Remove all acknowledged batches from the inflight queue
                  // removeObserved() removes the oldest inflight batch and returns it
                  while (landingZone.inflightCount() > 0) {
                    // Peek at the oldest inflight batch to check its offset
                    List<InflightBatch<P>> all = landingZone.peekAll();
                    if (all.isEmpty()) break;
                    // First item in peekAll is the oldest inflight batch
                    InflightBatch<P> oldest = all.get(0);
                    if (oldest.offsetId > ackedOffset) break;

                    InflightBatch<P> removed = landingZone.removeObserved();
                    if (removed != null) {
                      removed.ackPromise.complete(removed.offsetId);
                    }
                  }
                  BaseZerobusStream.this.notifyAll();
                }

                // Invoke offset callback if set
                if (options.offsetCallback().isPresent()) {
                  CompletableFuture.runAsync(
                          () -> options.offsetCallback().get().accept(ackedOffset), executor)
                      .exceptionally(
                          e -> {
                            logger.error(
                                "Exception in offset callback for offset {}", ackedOffset, e);
                            return null;
                          });
                }
                break;

              case CLOSE_STREAM_SIGNAL:
                if (options.recovery()) {
                  CloseStreamSignal closeSignal = response.getCloseStreamSignal();

                  // Determine wait duration: use user-configured value if set, otherwise server's
                  long serverWaitMs = 0;
                  if (closeSignal.hasDuration()) {
                    com.google.protobuf.Duration duration = closeSignal.getDuration();
                    serverWaitMs = duration.getSeconds() * 1000 + duration.getNanos() / 1_000_000;
                  }
                  final long waitMs = options.streamPausedMaxWaitTimeMs().orElse(serverWaitMs);

                  failureInfo.resetFailure(StreamFailureType.SERVER_CLOSED_STREAM);

                  // Capture watermark and transition to PAUSED state
                  // Ingestion can continue - records queue in landing zone for after recovery
                  synchronized (BaseZerobusStream.this) {
                    if (state == StreamState.OPENED || state == StreamState.FLUSHING) {
                      gracefulCloseWatermark = lastSentOffsetId;
                      setState(StreamState.PAUSED);
                      logger.info(
                          "Server signaled stream close, entering PAUSED state for {}ms, watermark={}",
                          waitMs,
                          gracefulCloseWatermark);
                    } else {
                      // Already in a terminal or transitional state, skip graceful close
                      break;
                    }
                  }

                  // Edge case: no batches ever sent
                  if (gracefulCloseWatermark < 0) {
                    logger.info(
                        "No pending batches during graceful close, triggering immediate recovery");
                    handleStreamFailed(StreamFailureType.SERVER_CLOSED_STREAM, Optional.empty());
                    break;
                  }

                  if (waitMs > 0) {
                    gracefulCloseTask =
                        CompletableFuture.runAsync(
                                () -> {
                                  long startTime = System.currentTimeMillis();
                                  synchronized (BaseZerobusStream.this) {
                                    // Wait for watermark batches to be acked (not all batches)
                                    while (state == StreamState.PAUSED
                                        && latestAckedOffsetId < gracefulCloseWatermark) {
                                      long elapsed = System.currentTimeMillis() - startTime;
                                      long remaining = waitMs - elapsed;
                                      if (remaining <= 0) {
                                        break;
                                      }
                                      try {
                                        BaseZerobusStream.this.wait(remaining);
                                      } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        break;
                                      }
                                    }
                                    if (latestAckedOffsetId >= gracefulCloseWatermark) {
                                      logger.info(
                                          "All watermark batches (up to offset {}) acked during graceful close, triggering recovery",
                                          gracefulCloseWatermark);
                                    } else {
                                      logger.info(
                                          "Graceful close timeout, watermark {} not fully acked (latest acked: {}), triggering recovery",
                                          gracefulCloseWatermark,
                                          latestAckedOffsetId);
                                    }
                                  }
                                  handleStreamFailed(
                                      StreamFailureType.SERVER_CLOSED_STREAM, Optional.empty());
                                },
                                executor)
                            .exceptionally(
                                e -> {
                                  logger.error("Error during graceful close wait", e);
                                  handleStreamFailed(
                                      StreamFailureType.SERVER_CLOSED_STREAM, Optional.empty());
                                  return null;
                                });
                  } else {
                    handleStreamFailed(StreamFailureType.SERVER_CLOSED_STREAM, Optional.empty());
                  }
                }
                break;

              default:
                throw new RuntimeException(new ZerobusException("Unknown response type"));
            }
          }

          @Override
          public void onError(Throwable t) {
            synchronized (BaseZerobusStream.this) {
              if (state == StreamState.CLOSED && !grpcStream.isPresent()) {
                logger.debug("Ignoring error on closed stream: {}", t.getMessage());
                return;
              }
            }
            Optional<Throwable> error = Optional.of(t);
            if (t instanceof StatusRuntimeException) {
              Status.Code code = ((StatusRuntimeException) t).getStatus().getCode();
              if (GrpcErrorHandling.isNonRetriable(code)) {
                error =
                    Optional.of(
                        new NonRetriableException(
                            "Non-retriable gRPC error: " + t.getMessage(), t));
              }
            }
            handleStreamFailed(StreamFailureType.SERVER_CLOSED_STREAM, error);
          }

          @Override
          public void onCompleted() {
            logger.debug("Server closed the stream");
            handleStreamFailed(StreamFailureType.SERVER_CLOSED_STREAM, Optional.empty());
          }
        };
  }

  // ==================== Utility Methods ====================

  private CompletableFuture<Void> runWithTimeout(
      long timeoutMs, Supplier<CompletableFuture<Void>> operation) {
    AtomicBoolean done = new AtomicBoolean(false);
    CompletableFuture<Void> future = operation.get();
    future.whenComplete(
        (r, e) -> {
          synchronized (done) {
            done.set(true);
            done.notifyAll();
          }
        });

    CompletableFuture<Void> timeout =
        CompletableFuture.runAsync(
            () -> {
              synchronized (done) {
                try {
                  done.wait(timeoutMs);
                  if (!done.get()) {
                    throw new RuntimeException(new TimeoutException("Operation timed out"));
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              }
            },
            executor);

    return CompletableFuture.anyOf(future, timeout).thenApply(r -> null);
  }

  private <T> CompletableFuture<T> runWithRetries(
      int maxRetries, String context, Supplier<CompletableFuture<T>> operation) {
    CompletableFuture<T> result = new CompletableFuture<>();
    int backoffMs = options.recovery() ? options.recoveryBackoffMs() : 0;

    class RetryHelper {
      void attempt(int n) {
        logger.debug("[{}] Attempt {}", context, n + 1);
        operation
            .get()
            .whenComplete(
                (r, e) -> {
                  if (e == null) {
                    result.complete(r);
                  } else if (e instanceof NonRetriableException
                      || e.getCause() instanceof NonRetriableException) {
                    result.completeExceptionally(e);
                  } else if (n < maxRetries - 1) {
                    CompletableFuture.runAsync(
                        () -> {
                          try {
                            Thread.sleep(backoffMs);
                          } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                          }
                          attempt(n + 1);
                        },
                        executor);
                  } else {
                    result.completeExceptionally(e);
                  }
                });
      }
    }
    new RetryHelper().attempt(0);
    return result;
  }

  private Throwable wrapException(Throwable e, String message) {
    if (e instanceof StatusRuntimeException) {
      Status.Code code = ((StatusRuntimeException) e).getStatus().getCode();
      if (GrpcErrorHandling.isNonRetriable(code)) {
        return new NonRetriableException(message + ": " + e.getMessage(), e);
      }
    }
    if (e instanceof NonRetriableException) {
      return new NonRetriableException(message + ": " + e.getMessage(), e);
    }
    return new ZerobusException(message + ": " + e.getMessage(), e);
  }
}
