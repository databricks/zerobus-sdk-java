package com.databricks.zerobus;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * MockedGrpcServer simulates server-side gRPC behavior for testing ZerobusSDK without needing a
 * real server.
 *
 * <p>It intercepts gRPC stream messages, processes them asynchronously, and sends responses back to
 * the client based on injected test scenarios.
 */
public class MockedGrpcServer {
  private static class AckRecord {
    final boolean success;
    final long offsetId;
    final long delayMs;
    final Throwable error;
    final boolean writeFailure;
    final boolean closeStreamSignal;

    AckRecord(
        boolean success,
        long offsetId,
        long delayMs,
        Throwable error,
        boolean writeFailure,
        boolean closeStreamSignal) {
      this.success = success;
      this.offsetId = offsetId;
      this.delayMs = delayMs;
      this.error = error;
      this.writeFailure = writeFailure;
      this.closeStreamSignal = closeStreamSignal;
    }
  }

  private static class CreateStreamResponse {
    final boolean success;
    final long delayMs;
    final boolean skip;
    final boolean writeFailure;

    CreateStreamResponse(boolean success, long delayMs, boolean skip, boolean writeFailure) {
      this.success = success;
      this.delayMs = delayMs;
      this.skip = skip;
      this.writeFailure = writeFailure;
    }
  }

  private final ExecutorService executorService;
  private final List<EphemeralStreamRequest> capturedMessages;
  private final List<AckRecord> injectedAckRecords;
  private final List<CreateStreamResponse> injectedCreateStreamResponses;
  private final BlockingQueue<EphemeralStreamRequest> messagesToProcess;

  private StreamObserver<EphemeralStreamResponse> ackSender;
  private long lastReceivedOffsetId = -1;
  private volatile boolean serverRunning = false;
  private volatile boolean streamReady = true;
  private Runnable streamReadyHandler;

  private final ClientCallStreamObserver<EphemeralStreamRequest> messageReceiver =
      new ClientCallStreamObserver<EphemeralStreamRequest>() {
        @Override
        public void onNext(EphemeralStreamRequest request) {
          synchronized (MockedGrpcServer.this) {
            capturedMessages.add(request);
            messagesToProcess.offer(request);
          }
        }

        @Override
        public void onError(Throwable t) {
          stopServerThread();
        }

        @Override
        public void onCompleted() {
          stopServerThread();
        }

        @Override
        public boolean isReady() {
          return streamReady;
        }

        @Override
        public void setOnReadyHandler(Runnable onReadyHandler) {
          streamReadyHandler = onReadyHandler;
        }

        @Override
        public void disableAutoInboundFlowControl() {}

        @Override
        public void request(int count) {}

        @Override
        public void setMessageCompression(boolean enable) {}

        @Override
        public void cancel(String message, Throwable cause) {}
      };

  public MockedGrpcServer() {
    this.executorService = Executors.newFixedThreadPool(2);
    this.capturedMessages = Collections.synchronizedList(new ArrayList<>());
    this.injectedAckRecords = Collections.synchronizedList(new ArrayList<>());
    this.injectedCreateStreamResponses = Collections.synchronizedList(new ArrayList<>());
    this.messagesToProcess = new LinkedBlockingQueue<>();
  }

  /** Initialize the mocked server with an ack sender and start processing messages. */
  public void initialize(StreamObserver<EphemeralStreamResponse> ackSender) {
    synchronized (this) {
      this.ackSender = ackSender;
      this.lastReceivedOffsetId = -1;
      this.messagesToProcess.clear();
      startServerThread();
    }
  }

  /** Inject a successful ack for a specific record offset with optional delay. */
  public void injectAckRecord(long offsetId, long delayMs) {
    injectedAckRecords.add(new AckRecord(true, offsetId, delayMs, null, false, false));
  }

  /** Inject a successful ack for a specific record offset. */
  public void injectAckRecord(long offsetId) {
    injectAckRecord(offsetId, 0);
  }

  /** Clear all injected ack records. */
  public void clearAckRecords() {
    synchronized (injectedAckRecords) {
      injectedAckRecords.clear();
    }
  }

  /** Inject a failed ingest record response. */
  public void injectFailIngestRecord(long offsetId, long delayMs, Throwable error) {
    injectedAckRecords.add(new AckRecord(false, offsetId, delayMs, error, false, false));
  }

  /** Inject a failed ingest record response. */
  public void injectFailIngestRecord(long offsetId) {
    injectFailIngestRecord(offsetId, 0, new RuntimeException("Ingest record failed"));
  }

  /** Inject a write failure for a specific record offset. */
  public void injectWriteFailureOfRecords(long offsetId, long delayMs) {
    injectedAckRecords.add(
        new AckRecord(
            false,
            offsetId,
            delayMs,
            new RuntimeException("IngestRecord write failure"),
            true,
            false));
  }

  /** Inject a write failure for a specific record offset. */
  public void injectWriteFailureOfRecords(long offsetId) {
    injectWriteFailureOfRecords(offsetId, 0);
  }

  /** Inject a non-retriable error for a specific record offset. */
  public void injectNonRetriableError(long offsetId, long delayMs) {
    io.grpc.StatusRuntimeException nonRetriableError =
        new io.grpc.StatusRuntimeException(
            io.grpc.Status.UNAUTHENTICATED.withDescription("Non-retriable gRPC error"));
    injectedAckRecords.add(
        new AckRecord(false, offsetId, delayMs, nonRetriableError, false, false));
  }

  /** Inject a non-retriable error for a specific record offset. */
  public void injectNonRetriableError(long offsetId) {
    injectNonRetriableError(offsetId, 0);
  }

  /** Inject a CloseStreamSignal for a specific record offset. */
  public void injectCloseStreamSignal(long offsetId, long delayMs) {
    injectedAckRecords.add(new AckRecord(true, offsetId, delayMs, null, false, true));
  }

  /** Inject a CloseStreamSignal for a specific record offset. */
  public void injectCloseStreamSignal(long offsetId) {
    injectCloseStreamSignal(offsetId, 0);
  }

  /** Inject a successful create stream response with delay. */
  public void injectCreateStreamSuccessWithDelay(long delayMs) {
    injectedCreateStreamResponses.add(new CreateStreamResponse(true, delayMs, false, false));
  }

  /** Inject a failed create stream response. */
  public void injectFailCreateStream() {
    injectedCreateStreamResponses.add(new CreateStreamResponse(false, 0, false, false));
  }

  /** Inject a skip create stream response (never sends response). */
  public void injectSkipCreateStreamResponse() {
    injectedCreateStreamResponses.add(new CreateStreamResponse(false, 0, true, false));
  }

  /** Inject a write failure for create stream. */
  public void injectWriteFailureCreateStream(long delayMs) {
    injectedCreateStreamResponses.add(new CreateStreamResponse(false, delayMs, false, true));
  }

  /** Inject a write failure for create stream. */
  public void injectWriteFailureCreateStream() {
    injectWriteFailureCreateStream(0);
  }

  /** Get all captured messages sent by the client. */
  public List<EphemeralStreamRequest> getCapturedMessages() {
    synchronized (capturedMessages) {
      return new ArrayList<>(capturedMessages);
    }
  }

  /** Get the message receiver for the client to write to. */
  public ClientCallStreamObserver<EphemeralStreamRequest> getMessageReceiver() {
    return messageReceiver;
  }

  /** Set stream readiness state. */
  public void setStreamReady(boolean ready) {
    boolean oldStreamReadyState = streamReady;
    streamReady = ready;

    if (streamReady && !oldStreamReadyState && streamReadyHandler != null) {
      streamReadyHandler.run();
    }
  }

  /** Destroy the mocked server and clean up resources. */
  public void destroy() {
    stopServerThread();
    executorService.shutdownNow();
    try {
      executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void startServerThread() {
    synchronized (this) {
      if (serverRunning) {
        return;
      }
      serverRunning = true;
    }

    executorService.submit(
        () -> {
          try {
            while (serverRunning) {
              EphemeralStreamRequest request = messagesToProcess.poll(100, TimeUnit.MILLISECONDS);
              if (request == null) {
                continue;
              }

              processMessage(request);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            // Server thread error, stop processing
          }
        });
  }

  private void stopServerThread() {
    synchronized (this) {
      serverRunning = false;
    }
  }

  private void processMessage(EphemeralStreamRequest request) throws InterruptedException {
    if (request.hasCreateStream()) {
      handleCreateStream();
    } else if (request.hasIngestRecord()) {
      handleIngestRecord(request.getIngestRecord().getOffsetId());
    }
  }

  private void handleCreateStream() throws InterruptedException {
    synchronized (injectedCreateStreamResponses) {
      if (injectedCreateStreamResponses.isEmpty()) {
        sendCreateStreamSuccess();
        return;
      }

      CreateStreamResponse response = injectedCreateStreamResponses.remove(0);
      if (response.skip) {
        return; // Never send response
      }

      if (response.delayMs > 0) {
        Thread.sleep(response.delayMs);
      }

      if (response.writeFailure) {
        throw new RuntimeException("CreateStream write failure");
      }

      if (response.success) {
        sendCreateStreamSuccess();
      } else {
        sendError(new RuntimeException("Create stream failed"));
      }
    }
  }

  private void handleIngestRecord(long offset) throws InterruptedException {
    if (offset != lastReceivedOffsetId + 1) {
      sendError(
          new RuntimeException(
              String.format(
                  "Invalid offset Id; expected %d but got %d", lastReceivedOffsetId + 1, offset)));
      return;
    }

    lastReceivedOffsetId = offset;

    synchronized (injectedAckRecords) {
      if (injectedAckRecords.isEmpty()) {
        // Default behavior: auto-ack all records when no specific behavior is injected
        sendAck(offset);
        return;
      }

      // Check if there's a specific ack record for this offset
      AckRecord matchingRecord = null;
      for (int i = 0; i < injectedAckRecords.size(); i++) {
        if (injectedAckRecords.get(i).offsetId == offset) {
          matchingRecord = injectedAckRecords.remove(i);
          break;
        }
      }

      if (matchingRecord != null) {
        // Process the specific injected behavior
        if (matchingRecord.delayMs > 0) {
          Thread.sleep(matchingRecord.delayMs);
        }

        if (matchingRecord.writeFailure) {
          throw new RuntimeException("IngestRecord write failure");
        }

        if (matchingRecord.closeStreamSignal) {
          sendCloseStreamSignal();
        } else if (matchingRecord.success) {
          sendAck(offset);
        } else {
          Throwable error =
              matchingRecord.error != null
                  ? matchingRecord.error
                  : new RuntimeException("Ingest failed");
          sendError(error);
        }
      }
      // Note: If no matching record found and injectedAckRecords is not empty,
      // do NOT send ack (this is intentional for tests that need to test lack of acks)
    }
  }

  private void sendCreateStreamSuccess() {
    if (ackSender != null) {
      EphemeralStreamResponse response =
          EphemeralStreamResponse.newBuilder()
              .setCreateStreamResponse(
                  CreateIngestStreamResponse.newBuilder().setStreamId("test-stream-id").build())
              .build();
      ackSender.onNext(response);
    }
  }

  private void sendAck(long offset) {
    if (ackSender != null) {
      EphemeralStreamResponse response =
          EphemeralStreamResponse.newBuilder()
              .setIngestRecordResponse(
                  IngestRecordResponse.newBuilder().setDurabilityAckUpToOffset(offset).build())
              .build();
      ackSender.onNext(response);
    }
  }

  private void sendCloseStreamSignal() {
    if (ackSender != null) {
      EphemeralStreamResponse response =
          EphemeralStreamResponse.newBuilder()
              .setCloseStreamSignal(CloseStreamSignal.newBuilder().build())
              .build();
      ackSender.onNext(response);
    }
  }

  private void sendError(Throwable error) {
    if (ackSender != null) {
      ackSender.onError(error);
    }
    stopServerThread();
  }
}
