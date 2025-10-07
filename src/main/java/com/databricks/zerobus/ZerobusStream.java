package com.databricks.zerobus;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.HashSet;
import java.util.Set;

import com.databricks.zerobus.ZerobusGrpc.ZerobusStub;
import com.databricks.zerobus.EphemeralStreamRequest;
import com.databricks.zerobus.EphemeralStreamResponse;
import com.databricks.zerobus.CreateIngestStreamRequest;
import com.databricks.zerobus.IngestRecordRequest;
import com.databricks.zerobus.RecordType;
import com.databricks.zerobus.IngestRecordResponse;

/**
 * Types of stream failures that can occur during ingestion.
 */
enum StreamFailureType {
  /** Unknown failure type */
  UNKNOWN,
  /** Server closed the stream */
  SERVER_CLOSED_STREAM,
  /** Failed while sending a message to the server */
  SENDING_MESSAGE,
  /** Server stopped responding to requests */
  SERVER_UNRESPONSIVE
}

/**
 * Tracks stream failure counts and types for recovery decisions.
 */
class StreamFailureInfo {
  private StreamFailureType _failureType = StreamFailureType.UNKNOWN;
  private int _failureCounts = 0;

  synchronized void logFailure(StreamFailureType streamFailureType) {
    if (streamFailureType == _failureType) {
      _failureCounts += 1;
    } else {
      _failureType = streamFailureType;
      _failureCounts = 1;
    }
  }

  synchronized void resetFailure(StreamFailureType streamFailureType) {
    if (_failureType == streamFailureType) {
      _failureCounts = 0;
      _failureType = StreamFailureType.UNKNOWN;
    }
  }

  synchronized int getFailureCounts() {
    return _failureCounts;
  }

  synchronized StreamFailureType getFailureType() {
    return _failureType;
  }
}

/**
 * Utility for classifying gRPC errors as retriable or non-retriable.
 * Non-retriable errors indicate issues that cannot be resolved by retrying
 * (e.g., invalid credentials, missing resources).
 */
class GrpcErrorHandling {
  private static final Set<Status.Code> NON_RETRIABLE_CODES = new HashSet<>();

  static {
    NON_RETRIABLE_CODES.add(Status.Code.INVALID_ARGUMENT);
    NON_RETRIABLE_CODES.add(Status.Code.NOT_FOUND);
    NON_RETRIABLE_CODES.add(Status.Code.UNAUTHENTICATED);
    NON_RETRIABLE_CODES.add(Status.Code.OUT_OF_RANGE);
  }

  /**
   * Determines if a gRPC status code represents a non-retriable error.
   *
   * @param code The gRPC status code to check
   * @return true if the error should not be retried
   */
  static boolean isNonRetriable(Status.Code code) {
    return NON_RETRIABLE_CODES.contains(code);
  }
}

/**
 * Internal record wrapper that tracks ingestion state.
 *
 * @param <T> The type of the protobuf message being ingested
 */
class Record<T extends Message> {
  long offsetId;
  final T record;
  final ByteString protoEncodedRecord;
  final CompletableFuture<Void> ackPromise;

  Record(long offsetId, T record, ByteString protoEncodedRecord, CompletableFuture<Void> ackPromise) {
    this.offsetId = offsetId;
    this.record = record;
    this.protoEncodedRecord = protoEncodedRecord;
    this.ackPromise = ackPromise;
  }
}

/**
 * Zerobus stream for ingesting records into a table.
 * Should be created using ZerobusSdk.createStream.
 */
public class ZerobusStream<RecordType extends Message> {
  private static final Logger logger = LoggerFactory.getLogger(ZerobusStream.class);

  // implicit ec: ExecutionContext - this is the ExecutionContext that client provides to run async operations (e.g.create stream async result processing)
  // zerobusStreamExecutor: ExecutionContext - This is used only for futures like timeout counter / stream recovery / stream unresponsiveness detection, so we don't block threads from customer's ExecutionContext
  //                 We have to use a separate executor (bounded) to make sure stream progress is not blocked

  private static final int CREATE_STREAM_TIMEOUT_MS = 15000;

  private ZerobusStub stub;
  final TableProperties<RecordType> tableProperties;
  private final ZerobusSdkStubFactory stubFactory;
  private final String serverEndpoint;
  final StreamConfigurationOptions options;
  private final ExecutorService zerobusStreamExecutor;
  private final ExecutorService ec;
  private final String workspaceId;
  private final String unityCatalogEndpoint;
  private final String clientId;
  private final String clientSecret;

  private StreamState state = StreamState.UNINITIALIZED;
  private Optional<String> streamId = Optional.empty();
  private Optional<ClientCallStreamObserver<EphemeralStreamRequest>> stream = Optional.empty();
  private Optional<CompletableFuture<String>> streamCreatedEvent = Optional.empty();

  // Sending records is asynchronus task which consumes records from recordsQueuedForSending and sends them to the server
  private final ArrayBlockingQueue<EphemeralStreamRequest> recordsQueuedForSending;

  // Here we store records which are not yet acknowledged by the server
  final ArrayBlockingQueue<Record<RecordType>> inflightRecords;

  // Populated just in case of hard failure, otherwise it's empty
  private final List<Record<RecordType>> unackedRecordsAfterStreamFailure = new ArrayList<>();

  private long latestRespondedOffsetId = -1;
  private long lastSentOffsetId = -1;
  private final StreamFailureInfo streamFailureInfo = new StreamFailureInfo();

  private final com.google.protobuf.DescriptorProtos.DescriptorProto descriptorProto;

  /**
   * Returns the ID of the stream.
   *
   * @return The ID of the stream.
   */
  public synchronized String getStreamId() {
    return streamId.orElse("");
  }

  /**
   * Returns the state of the stream.
   *
   * @return The state of the stream.
   */
  public synchronized StreamState getState() {
    return state;
  }

  /**
   * Returns the unacknowledged records after stream failure.
   *
   * @return The unacknowledged records after stream failure.
   */
  public Iterator<RecordType> getUnackedRecords() {
    List<RecordType> records = new ArrayList<>();
    for (Record<RecordType> record : unackedRecordsAfterStreamFailure) {
      records.add(record.record);
    }
    return records.iterator();
  }

  /**
   * Returns the table properties for this stream.
   *
   * @return The table properties.
   */
  public TableProperties<RecordType> getTableProperties() {
    return tableProperties;
  }

  /**
   * Returns the stream configuration options.
   *
   * @return The stream configuration options.
   */
  public StreamConfigurationOptions getOptions() {
    return options;
  }

  /**
   * Returns the OAuth client ID.
   *
   * @return The OAuth client ID.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Returns the OAuth client secret.
   *
   * @return The OAuth client secret.
   */
  public String getClientSecret() {
    return clientSecret;
  }

  private synchronized void setState(StreamState newState) {
    state = newState;
    this.notifyAll();
    logger.debug("Stream state changed to " + newState);
  }

  private CompletableFuture<Void> runWithTimeout(long timeoutMs, java.util.function.Supplier<CompletableFuture<Void>> getFuture) {
    AtomicBoolean done = new AtomicBoolean(false);
    CompletableFuture<Void> future = getFuture.get();

    future.whenComplete((result, error) -> {
      synchronized (done) {
        done.set(true);
        done.notifyAll();
      }
    });

    CompletableFuture<Void> timeoutFuture = CompletableFuture.runAsync(() -> {
      synchronized (done) {
        try {
          done.wait(timeoutMs);
          if (!done.get()) {
            throw new RuntimeException(new TimeoutException("Operation timed out!"));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }, zerobusStreamExecutor);

    return CompletableFuture.anyOf(future, timeoutFuture).thenApply(result -> null);
  }

  /**
   * Retries an operation with exponential backoff until success or max retries reached.
   *
   * <p>This method uses recursion through the RetryHelper inner class to avoid blocking
   * the caller thread. Each retry is scheduled asynchronously on the stream executor.
   *
   * @param maxRetries Maximum number of retry attempts
   * @param context Context string for logging
   * @param f Supplier that provides the operation to retry
   * @return CompletableFuture that completes with the operation result or error
   */
  private <T> CompletableFuture<T> runWithRetries(long maxRetries, String context, java.util.function.Supplier<CompletableFuture<T>> f) {
    CompletableFuture<T> resultPromise = new CompletableFuture<>();

    int backoffMs = options.recovery() ? options.recoveryBackoffMs() : 0;

    class RetryHelper {
      void tryNext(int attempt) {
        logger.debug("[" + context + "] Running attempt ... ");

        f.get().whenComplete((response, error) -> {
          if (error == null) {
            resultPromise.complete(response);
          } else if (error instanceof NonRetriableException || error.getCause() instanceof NonRetriableException) {
            // Non-retriable errors should fail immediately without retrying
            resultPromise.completeExceptionally(error);
          } else {
            if (attempt < maxRetries - 1) {
              // Schedule next retry after backoff period
              CompletableFuture.runAsync(() -> {
                logger.debug("[" + context + "] Retrying in " + backoffMs + " ms ... ");
                try {
                  Thread.sleep(backoffMs);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                tryNext(attempt + 1);
              }, zerobusStreamExecutor);
            } else {
              // Exhausted all retries
              resultPromise.completeExceptionally(error);
            }
          }
        });
      }
    }

    new RetryHelper().tryNext(0);
    return resultPromise;
  }

  private void failStream(Throwable error) {
    synchronized (this) {
      if (stream.isPresent()) {
        try {
          stream.get().onError(error);
        } catch (Exception e) {
          // Ignore
        }

        stream = Optional.empty();
        streamId = Optional.empty();
      }
    }
  }

  private CompletableFuture<Void> createStream() {
    CompletableFuture<Void> createStreamDone = new CompletableFuture<>();

    int timeoutMs = options.recovery() ? options.recoveryTimeoutMs() : CREATE_STREAM_TIMEOUT_MS;

    latestRespondedOffsetId = -1;
    lastSentOffsetId = -1;
    streamId = Optional.empty();
    stream = Optional.empty();
    streamCreatedEvent = Optional.empty();

    runWithTimeout(timeoutMs, () -> {
      CompletableFuture<Void> createStreamTry = new CompletableFuture<>();

      // Generate a fresh token for this stream creation attempt
      try {
        String token = TokenFactory.getZerobusToken(
            tableProperties.getTableName(),
            workspaceId,
            unityCatalogEndpoint,
            clientId,
            clientSecret);

        // Create a new stub with the fresh token
        stub = stubFactory.createStub(
            serverEndpoint,
            true,
            tableProperties.getTableName(),
            token);

        logger.debug("Generated new token and created stub for stream");
      } catch (NonRetriableException e) {
        createStreamTry.completeExceptionally(e);
        return createStreamTry;
      }

      // Create the gRPC stream with the new stub
      streamCreatedEvent = Optional.of(new CompletableFuture<>());
      stream = Optional.of(
        (ClientCallStreamObserver<EphemeralStreamRequest>) stub.ephemeralStream(ackReceiver)
      );

      logger.debug("Creating ephemeral stream for table " + tableProperties.getTableName());

      // Create the initial request
      EphemeralStreamRequest createStreamRequest = EphemeralStreamRequest.newBuilder()
        .setCreateStream(
          CreateIngestStreamRequest.newBuilder()
            .setTableName(tableProperties.getTableName())
            .setDescriptorProto(ByteString.copyFrom(descriptorProto.toByteArray()))
            .setRecordType(com.databricks.zerobus.RecordType.PROTO)
            .build()
        )
        .build();

      // Send the CreateStreamRequest
      try {
        sendMessage(createStreamRequest);
      } catch (Exception exception) {
        failStream(exception);
        createStreamTry.completeExceptionally(exception);
        return createStreamTry;
      }

      streamCreatedEvent.get().whenComplete((id, e) -> {
        if (e == null) {
          streamId = Optional.of(id);
          recordsSenderTask.start();
          createStreamTry.complete(null);
        } else if (e instanceof ZerobusException) {
          failStream(e);
          streamId = Optional.empty();
          streamCreatedEvent = Optional.empty();
          stream = Optional.empty();
          createStreamTry.completeExceptionally(e);
        } else {
          failStream(e);
          streamId = Optional.empty();
          streamCreatedEvent = Optional.empty();
          stream = Optional.empty();
          createStreamTry.completeExceptionally(new ZerobusException(e.getMessage(), e));
        }
      });

      return createStreamTry;
    }).whenComplete((result, e) -> {
      if (e == null) {
        createStreamDone.complete(null);
      } else {
        failStream(e);
        Throwable ex;
        if (e instanceof StatusRuntimeException) {
          Status.Code code = ((StatusRuntimeException) e).getStatus().getCode();
          if (GrpcErrorHandling.isNonRetriable(code)) {
            ex = new NonRetriableException("Non-retriable gRPC error during stream creation: " + e.getMessage(), e);
          } else {
            ex = new ZerobusException("Stream creation failed: " + e.getMessage(), e);
          }
        } else if (e instanceof NonRetriableException) {
          ex = new NonRetriableException("Stream creation failed: " + e.getMessage(), e);
        } else {
          ex = new ZerobusException("Stream creation failed: " + e.getMessage(), e);
        }
        createStreamDone.completeExceptionally(ex);
      }
    });

    return createStreamDone;
  }

  CompletableFuture<Void> initialize() {
    CompletableFuture<Void> initializeDone = new CompletableFuture<>();

    synchronized (this) {
      if (state != StreamState.UNINITIALIZED) {
        logger.error("Stream cannot be initialized/opened more than once");
        initializeDone.completeExceptionally(
          new ZerobusException("Stream cannot be initialized/opened more than once")
        );
        return initializeDone;
      }
    }

    int retries = options.recovery() ? options.recoveryRetries() : 1;

    runWithRetries(retries, "CreateStream", () -> createStream()).whenComplete((result, e) -> {
      if (e == null) {
        setState(StreamState.OPENED);
        serverUnresponsivenessDetectionTask.start();
        logger.info("Stream created successfully with id " + streamId.get());
        initializeDone.complete(null);
      } else {
        setState(StreamState.FAILED);
        logger.error("Failed to create stream: ", e);
        if (e instanceof ZerobusException) {
          initializeDone.completeExceptionally(e);
        } else {
          initializeDone.completeExceptionally(new ZerobusException("Stream creation failed: " + e.getMessage(), e));
        }
      }
    });

    return initializeDone;
  }

  /**
   * Closes the stream and cleans up resources.
   *
   * @param hardFailure If true, marks stream as FAILED and saves unacked records for potential retry
   * @param exception The exception that caused the failure (if any)
   */
  private void closeStream(boolean hardFailure, Optional<ZerobusException> exception) {
    synchronized (this) {
      logger.debug("Closing stream, hardFailure: " + hardFailure);

      if (hardFailure && exception.isPresent()) {
        // CRITICAL: Atomically mark stream as FAILED before processing unacked records.
        // This prevents race conditions where clients see errors but unackedRecords is empty.
        setState(StreamState.FAILED);
      }

      recordsQueuedForSending.clear();
      recordsSenderTask.cancel();

      try {
        if (stream.isPresent()) {
          stream.get().onCompleted();
        }
      } catch (Exception e) {
        // Ignore errors during stream cleanup - stream may already be closed
        logger.debug("Error while closing stream: " + e.getMessage());
      }

      // For hard failures, preserve unacked records so they can be retried via recreateStream()
      if (hardFailure) {
        serverUnresponsivenessDetectionTask.cancel();
        logger.debug("Stream closing: Failing all unacked records");

        while (!inflightRecords.isEmpty()) {
          try {
            Record<RecordType> record = inflightRecords.take();
            unackedRecordsAfterStreamFailure.add(record);
            record.ackPromise.completeExceptionally(exception.orElse(new ZerobusException("Stream failed")));
            this.notifyAll();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }

      stream = Optional.empty();
      streamCreatedEvent = Optional.empty();
      streamId = Optional.empty();

      this.notifyAll();
    }

    // Wait for background tasks to fully stop before returning.
    // This ensures clean shutdown and prevents resource leaks.
    recordsSenderTask.waitUntilStopped();
    if (hardFailure) {
      serverUnresponsivenessDetectionTask.waitUntilStopped();
    }
  }

  private CompletableFuture<Void> closeStreamAsync(boolean hardFailure, Optional<ZerobusException> exception) {
    return CompletableFuture.runAsync(() -> closeStream(hardFailure, exception), zerobusStreamExecutor);
  }

  private void enqueueRecordsForResending() {
    synchronized (this) {
      if (state != StreamState.RECOVERING) {
        return;
      }

      Iterator<Record<RecordType>> recordsIterator = inflightRecords.iterator();

      while (recordsIterator.hasNext()) {
        Record<RecordType> record = recordsIterator.next();

        lastSentOffsetId += 1;
        long offsetId = lastSentOffsetId;

        record.offsetId = offsetId;

        EphemeralStreamRequest recordRequest = EphemeralStreamRequest.newBuilder()
          .setIngestRecord(
            IngestRecordRequest.newBuilder()
              .setOffsetId(offsetId)
              .setProtoEncodedRecord(record.protoEncodedRecord)
              .build()
          )
          .build();

        try {
          recordsQueuedForSending.put(recordRequest);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  /**
   * Attempts to recover a failed stream by recreating it and resending unacked records.
   *
   * <p>This method:
   * <ol>
   *   <li>Closes the current stream without marking it as hard failure</li>
   *   <li>Creates a new stream with the same configuration</li>
   *   <li>Re-enqueues all unacknowledged records for sending</li>
   * </ol>
   *
   * @return CompletableFuture that completes when recovery succeeds or fails
   */
  private CompletableFuture<Void> recoverStream() {
    CompletableFuture<Void> recoverStreamDone = new CompletableFuture<>();

    CompletableFuture.runAsync(() -> {
      if (!options.recovery()) {
        logger.debug("Stream recovery is disabled");
        recoverStreamDone.completeExceptionally(new ZerobusException("Stream recovery is disabled"));
      } else {
        logger.warn("Stream broken! Running stream recovery for stream id '" + streamId.orElse("unknown") + "' ... ");

        // Close the broken stream but don't mark as hard failure since we're attempting recovery
        closeStream(false, Optional.empty());

        synchronized (this) {
          int retries = options.recoveryRetries();
          // Reduce remaining retries based on consecutive failures of the same type
          int leftRetries = Math.max(0, retries - streamFailureInfo.getFailureCounts() + 1);

          if (leftRetries == 0) {
            logger.debug("Stream recovery failed: Run out of retries");
            recoverStreamDone.completeExceptionally(new ZerobusException("Stream recovery failed"));
            return;
          }

          logger.debug("Stream recovery: Running with " + leftRetries + " / " + retries + " retries left");

          runWithRetries(leftRetries, "RecoverStream", () -> {
            CompletableFuture<Void> recoverStreamTry = new CompletableFuture<>();

            createStream().whenComplete((result, e) -> {
              if (e != null) {
                logger.debug("Stream recovery: Failed to create stream: " + e.getMessage());
                recoverStreamTry.completeExceptionally(e);
              } else {
                enqueueRecordsForResending();
                recoverStreamTry.complete(null);
              }
            });

            return recoverStreamTry;
          }).whenComplete((result, e) -> {
            if (e == null) {
              logger.info("Stream recovery completed successfully. New stream id: " + streamId.get());
              recoverStreamDone.complete(null);
            } else {
              logger.error("Stream recovery failed: " + e.getMessage(), e);
              recoverStreamDone.completeExceptionally(e);
            }
          });
        }
      }
    }, zerobusStreamExecutor);

    return recoverStreamDone;
  }

  private void handleStreamFailed(StreamFailureType streamFailureType, Optional<Throwable> error) {

    Optional<ZerobusException> exception;
    if (error.isPresent()) {
      Throwable e = error.get();
      if (e instanceof ZerobusException) {
        exception = Optional.of((ZerobusException) e);
      } else {
        exception = Optional.of(new ZerobusException("Stream failed: " + e.getMessage(), e));
      }
    } else {
      exception = Optional.of(new ZerobusException("Stream failed"));
    }

    synchronized (this) {

      if (state == StreamState.FAILED || state == StreamState.UNINITIALIZED || state == StreamState.RECOVERING) {
        // UNINITIALIZED -> Stream failed during creation
        // FAILED -> Stream already failed (don't handle it twice)
        // RECOVERING -> Stream is recovering from a failure, no action needed

        if (state == StreamState.UNINITIALIZED && streamCreatedEvent.isPresent()) {
          streamCreatedEvent.get().completeExceptionally(exception.get());
        }

        return;
      }

      if (state == StreamState.CLOSED && !error.isPresent()) {
        // Stream failed after closed, but without exception - that's expected (stream closed gracefully)
        return;
      }

      if (error.isPresent()) {
        logger.error("Stream failed: " + error.get().getMessage(), error.get());
      }

      // Check if this is a non-retriable error - if so, don't attempt recovery
      if (error.isPresent() && error.get() instanceof NonRetriableException) {
        closeStreamAsync(true, exception);
        return;
      }

      streamFailureInfo.logFailure(streamFailureType);

      // Stream is open or flushing, try to recover it
      setState(StreamState.RECOVERING);

      recoverStream().whenComplete((result, e) -> {
        if (e == null) {
          setState(StreamState.OPENED);
          logger.info("Stream recovered successfully with id " + streamId.get());
        } else {
          logger.error("Stream recovery failed", e);
          closeStream(true, exception);
        }
      });
    }
  }

  private CompletableFuture<Void> handleStreamFailedAsync(StreamFailureType streamFailureType, Optional<Throwable> error) {
    return CompletableFuture.runAsync(() -> handleStreamFailed(streamFailureType, error), zerobusStreamExecutor);
  }

  // Task that checks if server is responsive (time it takes for server to ack a record)
  // Task is created once during initialize() and it's shutdown when stream is closed finally
  //     (e.g. close() is called or stream can't be recovered)
  private BackgroundTask serverUnresponsivenessDetectionTask;

  private void initServerUnresponsivenessDetectionTask() {
    serverUnresponsivenessDetectionTask = new BackgroundTask(
    cancellationToken -> {
      long taskIterationStartTime = System.currentTimeMillis();
      synchronized (ZerobusStream.this) {
        switch (state) {
          case UNINITIALIZED:
          case CLOSED:
          case FAILED:
            break;

          case RECOVERING:
            logger.debug("Server unresponsiveness detection task: Waiting for stream to finish recovering");
            try {
              ZerobusStream.this.wait();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            break;

          case OPENED:
          case FLUSHING:
            if (inflightRecords.isEmpty()) {
              logger.debug("Server unresponsiveness detection task: Waiting for some records to be ingested");
              try {
                ZerobusStream.this.wait();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            } else {
              // STREAM IS OPENED OR FLUSHING AND THERE ARE RECORDS IN THE QUEUE - CHECK IF SERVER IS RESPONSIVE
              long latestRespondedOffsetIdBefore = latestRespondedOffsetId;
              boolean serverResponsive = false;
              boolean serverResponsiveTimeout = false;

              while (!serverResponsive && !serverResponsiveTimeout) {
                if (latestRespondedOffsetIdBefore != latestRespondedOffsetId) {
                  serverResponsive = true;
                } else {
                  long remainingTime = options.serverLackOfAckTimeoutMs() - (System.currentTimeMillis() - taskIterationStartTime);

                  if (remainingTime <= 0) {
                    // We don't want to block here, since this potentially can close the stream, which will wait for this task to finish (deadlock)
                    handleStreamFailedAsync(
                      StreamFailureType.SERVER_UNRESPONSIVE,
                      Optional.of(new ZerobusException("Server is unresponsive"))
                    );
                    serverResponsiveTimeout = true;
                  } else {
                    try {
                      ZerobusStream.this.wait(remainingTime);
                      if (cancellationToken.isDone()) {
                        // In case of a stream close, break the loop so that it doesn't hang waiting for the timeout.
                        serverResponsive = true;
                      }
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                  }
                }
              }
            }
            break;
        }
      }
    },
    error -> {
      // This should never happen (task won't throw any errors), but if it does, we need to handle it
      // and it probably won't be recoverable
      logger.error("Server unresponsiveness detection task failed: " + error.getMessage(), error);

      closeStreamAsync(
        true,
        Optional.of(
          new ZerobusException(
            "Server unresponsiveness detection task failed: " + error.getMessage(),
            error
          )
        )
      );
    },
    zerobusStreamExecutor
  );
  }

  // Task that consumes records from recordsQueuedForSending and sends them to the server
  // This task is restarted each time stream is recovered/restarted
  private BackgroundTask recordsSenderTask;

  private void initRecordsSenderTask() {
    recordsSenderTask = new BackgroundTask(
    cancellationToken -> {
      // Check if there are records to send
      Optional<EphemeralStreamRequest> recordRequest;
      synchronized (ZerobusStream.this) {
        switch (state) {
          case OPENED:
          case FLUSHING:
            if (recordsQueuedForSending.isEmpty()) {
              try {
                ZerobusStream.this.wait();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              recordRequest = Optional.empty();
            } else {
              try {
                recordRequest = Optional.of(recordsQueuedForSending.take());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                recordRequest = Optional.empty();
              }
            }
            break;
          case CLOSED:
            if (recordsQueuedForSending.isEmpty()) {
              recordRequest = Optional.empty();
            } else {
              try {
                recordRequest = Optional.of(recordsQueuedForSending.take());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                recordRequest = Optional.empty();
              }
            }
            break;
          default:
            recordRequest = Optional.empty();
            break;
        }
      }

      // If we have a record, wait for stream to be ready and send it
      if (recordRequest.isPresent()) {
        if (stream.isPresent()) {
          ClientCallStreamObserver<EphemeralStreamRequest> strm = stream.get();
          // Wait for stream to be ready
          synchronized (ZerobusStream.this) {
            while (!strm.isReady() && !cancellationToken.isDone()) {
              try {
                ZerobusStream.this.wait();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
            }
          }
          if (!cancellationToken.isDone()) {
            // Send the record
            try {
              sendMessage(recordRequest.get());
              streamFailureInfo.resetFailure(StreamFailureType.SENDING_MESSAGE);
            } catch (Exception ex) {
              logger.error("Error while sending record: " + ex.getMessage(), ex);

              // Use async to avoid deadlock: handleStreamFailed() may call closeStream()
              // which waits for this task to stop.
              handleStreamFailedAsync(StreamFailureType.SENDING_MESSAGE, Optional.of(ex));

              // Wait for state change before continuing. This prevents repeatedly attempting
              // to send the next record which would likely fail with the same error.
              // The task will be restarted after recovery (or shut down if recovery fails).
              synchronized (ZerobusStream.this) {
                while ((state == StreamState.OPENED || state == StreamState.FLUSHING) && !cancellationToken.isDone()) {
                  try {
                    ZerobusStream.this.wait();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                  }
                }
              }
            }
          }
        }
      }
      // No record available, continue to next iteration
    },
    error -> {
      // This should never happen (task won't throw any errors), but if it does, we need to handle it
      // and it probably won't be recoverable
      logger.error("Records sender task failed: " + error.getMessage(), error);

      closeStreamAsync(
        true,
        Optional.of(
          new ZerobusException(
            "Records sender task failed: " + error.getMessage(),
            error
          )
        )
      );
    },
    zerobusStreamExecutor
  );
  }

  private ClientResponseObserver<EphemeralStreamRequest, EphemeralStreamResponse> ackReceiver;

  private void initAckReceiver() {
    ackReceiver = new ClientResponseObserver<EphemeralStreamRequest, EphemeralStreamResponse>() {
      // Track state for the receiver
      private Optional<String> ackReceiverStreamId = Optional.empty();

      @Override
      public void beforeStart(ClientCallStreamObserver<EphemeralStreamRequest> requestStream) {
        requestStream.setOnReadyHandler(() -> {
          synchronized (ZerobusStream.this) {
            ZerobusStream.this.notifyAll();
          }
        });
      }

      @Override
      public void onNext(EphemeralStreamResponse response) {
        switch (response.getPayloadCase()) {
          // *** Create stream response ***
          case CREATE_STREAM_RESPONSE:
            ackReceiverStreamId = Optional.of(
              response.getCreateStreamResponse().getStreamId().isEmpty() ?
                null : response.getCreateStreamResponse().getStreamId()
            );
            if (!ackReceiverStreamId.isPresent() || ackReceiverStreamId.get() == null) {
              throw new RuntimeException(new ZerobusException("Invalid response from server: stream id is missing"));
            }
            logger.debug("Stream created with id " + ackReceiverStreamId.get());
            streamCreatedEvent.get().complete(ackReceiverStreamId.get());
            break;

          // *** Ingest record response (durability ack) ***
          case INGEST_RECORD_RESPONSE:
            String streamIdForReceiver = ackReceiverStreamId.orElseThrow(() ->
              new RuntimeException(new ZerobusException("Invalid response from server: expected stream id but got record ack"))
            );
            long ackedOffsetId = response.getIngestRecordResponse().getDurabilityAckUpToOffset();
            logger.debug("Acked offset " + ackedOffsetId);

            synchronized (ZerobusStream.this) {

              // Edge case: Stream was recovered/recreated while ack was in flight.
              // Ignore stale acks from old stream to avoid incorrectly completing promises.
              if (!streamId.isPresent() || !streamIdForReceiver.equals(streamId.get())) {
                return;
              }

              // Receiving an ack proves the server is responsive and connection is healthy
              streamFailureInfo.resetFailure(StreamFailureType.SERVER_CLOSED_STREAM);
              streamFailureInfo.resetFailure(StreamFailureType.SERVER_UNRESPONSIVE);

              latestRespondedOffsetId = Math.max(latestRespondedOffsetId, ackedOffsetId);

              // Complete promises for all records up to and including the acked offset.
              // Server guarantees durability for all records <= ackedOffsetId.
              boolean processingDone = false;
              while (!processingDone) {
                if (inflightRecords.isEmpty()) {
                  processingDone = true;
                } else {
                  Record<RecordType> record = inflightRecords.peek();

                  if (record.offsetId > ackedOffsetId) {
                    // This record hasn't been acked yet
                    processingDone = true;
                  } else {
                    record.ackPromise.complete(null);
                    try {
                      inflightRecords.take();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      break;
                    }
                  }
                }
              }

              ZerobusStream.this.notifyAll();
            }

            // Invoke user callback asynchronously to avoid blocking the gRPC receiver thread.
            // Exceptions in user code should not affect stream operation.
            if (options.ackCallback().isPresent()) {
              CompletableFuture.runAsync(() -> {
                options.ackCallback().get().accept(response.getIngestRecordResponse());
              }, ec).exceptionally(e -> {
                logger.error(
                  "Exception in async ack_callback for offset " + response.getIngestRecordResponse().getDurabilityAckUpToOffset(),
                  e
                );
                return null;
              });
            }
            break;

          // *** Close stream signal ***
          case CLOSE_STREAM_SIGNAL:
            if (options.recovery()) {
              double durationMs = 0.0;
              if (response.getCloseStreamSignal().hasDuration()) {
                durationMs = response.getCloseStreamSignal().getDuration().getSeconds() * 1000.0 +
                  response.getCloseStreamSignal().getDuration().getNanos() / 1000000.0;
              }
              logger.info(String.format("Server will close the stream in %.3fms. Triggering stream recovery.", durationMs));
              handleStreamFailed(StreamFailureType.SERVER_CLOSED_STREAM, Optional.empty());
            }
            break;

          // *** Unknown response ***
          default:
            throw new RuntimeException(new ZerobusException("Invalid response from server"));
        }
      }

      @Override
      public void onError(Throwable t) {
        Optional<Throwable> error = Optional.of(t);

        if (t instanceof StatusRuntimeException) {
          Status.Code code = ((StatusRuntimeException) t).getStatus().getCode();
          if (GrpcErrorHandling.isNonRetriable(code)) {
            error = Optional.of(
              new NonRetriableException("Non-retriable gRPC error: " + ((StatusRuntimeException) t).getStatus(), t)
            );
          }
        }

        handleStreamFailed(StreamFailureType.SERVER_CLOSED_STREAM, error);
      }

      @Override
      public void onCompleted() {
        logger.debug("Server called close on the stream");
        handleStreamFailed(StreamFailureType.SERVER_CLOSED_STREAM, Optional.empty());
      }
    };
  }

  private void sendMessage(EphemeralStreamRequest message) throws Exception {
    stream.get().onNext(message);
  }

  /**
   * Ingests a record into the stream.
   *
   * @param record The record to ingest.
   * @return An IngestRecordResult containing two futures:
   *   - recordAccepted: completes when the SDK accepts and queues the record for processing
   *   - writeCompleted: completes when the server acknowledges the record has been durably stored
   *   If either future raises an exception, the record most probably was not acknowledged,
   *   but it is also possible that the server acknowledged the record but the response was lost.
   *   In this case client should decide whether to retry the record or not.
   */
  public IngestRecordResult ingestRecord(RecordType record) {
    CompletableFuture<Void> enqueuePromise = new CompletableFuture<>();
    CompletableFuture<Void> durabilityPromise = new CompletableFuture<>();

    CompletableFuture.runAsync(() -> {
      synchronized (this) {
        // Wait until there is space in the queue
        boolean recordQueueFull = true;
        while (recordQueueFull) {
          switch (state) {
            case RECOVERING:
            case FLUSHING:
              logger.debug("Ingest record: Waiting for stream " + streamId.orElse("") + " to finish recovering/flushing");
              try {
                this.wait();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
              break;
            case FAILED:
            case CLOSED:
            case UNINITIALIZED:
              logger.error("Cannot ingest record when stream is closed or not opened for stream ID " + streamId.orElse("unknown"));
              throw new RuntimeException(new ZerobusException(
                "Cannot ingest record when stream is closed or not opened for stream ID " + streamId.orElse("unknown")
              ));
            case OPENED:
              if (inflightRecords.remainingCapacity() > 0) {
                recordQueueFull = false;
              } else {
                logger.debug("Ingest record: Waiting for space in the queue");
                try {
                  this.wait();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              }
              break;
          }
        }

        ByteString protoEncodedRecord = ByteString.copyFrom(record.toByteArray());
        lastSentOffsetId += 1;
        long offsetId = lastSentOffsetId;

        try {
          inflightRecords.put(new Record<>(offsetId, record, protoEncodedRecord, durabilityPromise));

          recordsQueuedForSending.put(
            EphemeralStreamRequest.newBuilder()
              .setIngestRecord(
                IngestRecordRequest.newBuilder()
                  .setOffsetId(offsetId)
                  .setProtoEncodedRecord(protoEncodedRecord)
                  .build()
              )
              .build()
          );
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }

        this.notifyAll();
      }
    }, zerobusStreamExecutor).whenComplete((result, ex) -> {
      if (ex == null) {
        enqueuePromise.complete(null);
      } else {
        enqueuePromise.completeExceptionally(ex);
        durabilityPromise.completeExceptionally(ex);
      }
    });

    return new IngestRecordResult(enqueuePromise, durabilityPromise);
  }

  /**
   * Flushes the stream, waiting for all queued records to be acknowledged by the server.
   * The stream doesn't close after flushing.
   *
   * @throws ZerobusException If the stream is not opened.
   */
  public void flush() throws ZerobusException {
    synchronized (this) {
      logger.debug("Flushing stream ...");

      try {
        if (state == StreamState.UNINITIALIZED) {
          logger.error("Cannot flush stream when it is not opened");
          throw new ZerobusException("Cannot flush stream when it is not opened");
        }

        while (state == StreamState.RECOVERING) {
          logger.debug("Flushing stream: Waiting for stream to finish recovering");
          try {
            this.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZerobusException("Error while flushing stream", e);
          }
        }

        if (state == StreamState.OPENED) {
          setState(StreamState.FLUSHING);
        }

        long startTime = System.currentTimeMillis();

        boolean recordsFlushed = false;
        while (!recordsFlushed) {
          if (state == StreamState.FAILED) {
            logger.error("Stream failed, cannot flush");
            throw new ZerobusException("Stream failed, cannot flush");
          } else {
            if (inflightRecords.isEmpty()) {
              recordsFlushed = true;
            } else {
              long remainingTime = options.flushTimeoutMs() - (System.currentTimeMillis() - startTime);

              if (remainingTime <= 0) {
                logger.error("Flushing stream timed out");
                throw new ZerobusException("Flushing stream timed out");
              }

              try {
                logger.debug("Waiting for " + remainingTime + "ms to flush stream ...");
                this.wait(remainingTime);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Error while flushing stream: " + e.getMessage(), e);
                throw new ZerobusException("Error while flushing stream", e);
              }
            }
          }
        }

        if (!inflightRecords.isEmpty()) {
          logger.error("Flushing stream timed out");
          throw new ZerobusException("Flushing stream timed out");
        }

        logger.info("All records have been flushed");
      } finally {
        if (state == StreamState.FLUSHING) {
          setState(StreamState.OPENED);
        }
      }
    }
  }

  /**
   * Closes the stream, while first flushing all queued records.
   * Once a stream is closed, it cannot be reopened.
   *
   * @throws ZerobusException If the stream is not opened.
   */
  public void close() throws ZerobusException {
    boolean readyToClose = false;
    synchronized (this) {
      while (!readyToClose) {
        switch (state) {
          case UNINITIALIZED:
            logger.error("Cannot close stream when it is not opened");
            throw new ZerobusException("Cannot close stream when it is not opened");
          case FAILED:
            logger.error("Stream failed and cannot be gracefully closed");
            throw new ZerobusException("Stream failed and cannot be gracefully closed");
          case CLOSED:
            // Idempotent operation
            logger.debug("Close stream: Stream is already closed");
            return;
          case FLUSHING:
          case RECOVERING:
            // Wait until the stream is flushed or recovering
            logger.debug("Close stream: Waiting for stream to finish flushing/recovering");
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

    Optional<ZerobusException> receivedException = Optional.empty();
    try {
      flush();
    } catch (ZerobusException ex) {
      // Case 1: The exception is already the type we want.
      receivedException = Optional.of(ex);
      throw ex; // Re-throw the original exception.
    } catch (Exception otherEx) {
      // Case 2: Any other non-fatal exception.
      // Wrap the unexpected exception in a new ZerobusException.
      ZerobusException wrappedEx = new ZerobusException("Underlying failure during flush", otherEx);
      receivedException = Optional.of(wrappedEx);
      throw wrappedEx;
    } finally {
      closeStream(true, receivedException);
    }

    logger.info("Stream gracefully closed");
  }

  public ZerobusStream(
      ZerobusStub stub,
      TableProperties<RecordType> tableProperties,
      ZerobusSdkStubFactory stubFactory,
      String serverEndpoint,
      String workspaceId,
      String unityCatalogEndpoint,
      String clientId,
      String clientSecret,
      StreamConfigurationOptions options,
      ExecutorService zerobusStreamExecutor,
      ExecutorService ec) {
    this.stub = stub;
    this.tableProperties = tableProperties;
    this.stubFactory = stubFactory;
    this.serverEndpoint = serverEndpoint;
    this.workspaceId = workspaceId;
    this.unityCatalogEndpoint = unityCatalogEndpoint;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.options = options;
    this.zerobusStreamExecutor = zerobusStreamExecutor;
    this.ec = ec;

    this.recordsQueuedForSending = new ArrayBlockingQueue<>(options.maxInflightRecords());
    this.inflightRecords = new ArrayBlockingQueue<>(options.maxInflightRecords());
    this.descriptorProto = tableProperties.getDescriptorProto();

    // Initialize background tasks and observers
    initServerUnresponsivenessDetectionTask();
    initRecordsSenderTask();
    initAckReceiver();
  }
}
